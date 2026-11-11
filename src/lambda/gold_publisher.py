import boto3
import hashlib
import json
import os
import time

athena = boto3.client("athena")
lambda_client = boto3.client("lambda")

WORKGROUP = "healthcare-lakehouse"
DATABASE = "healthcare_gold"
ALLOWED_BATCH = "encounter_batch_001"
BATCH_MANAGER_FUNCTION = os.environ.get(
    "BATCH_MANAGER_FUNCTION", "healthcare-lakehouse-batch-manager"
)


def batch_manager(payload):
    response = lambda_client.invoke(
        FunctionName=BATCH_MANAGER_FUNCTION,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    body = json.loads(response["Payload"].read())
    if response.get("FunctionError"):
        raise RuntimeError(f"Batch manager rejected request: {body}")
    if response.get("StatusCode") != 200 or not isinstance(body, dict):
        raise RuntimeError(f"Unexpected batch manager response: {body}")
    return body


def wait_for_query(query_id, context, *, is_merge=False):
    while context.get_remaining_time_in_millis() > 10000:
        execution = athena.get_query_execution(QueryExecutionId=query_id)
        status = execution["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            return query_id
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(
                f"Athena {state} for {query_id}: "
                + status.get("StateChangeReason", "")
            )
        time.sleep(2)

    # A MERGE may already have committed. Never assume cancellation rolled it back.
    # Leave the registered ID available for explicit recovery assessment.
    if not is_merge:
        athena.stop_query_execution(QueryExecutionId=query_id)
    raise TimeoutError(
        f"Athena {'MERGE' if is_merge else 'query'} still unresolved: {query_id}"
    )


def run_query(sql, context):
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
    )
    return wait_for_query(response["QueryExecutionId"], context)


def get_scalar(query_id):
    response = athena.get_query_results(QueryExecutionId=query_id)
    rows = response["ResultSet"]["Rows"]
    if len(rows) != 2:
        raise RuntimeError("Unexpected Athena result")
    return int(rows[1]["Data"][0]["VarCharValue"])


def publish_merge(merge_sql, batch_name, execution_arn, context):
    # Deterministic for this exact execution and exact SQL. A retry with the same
    # parameters receives the original Athena QueryExecutionId, not a new MERGE.
    operation = json.dumps(
        {
            "batch_name": batch_name,
            "execution_arn": execution_arn,
            "layer": "gold",
            "workgroup": WORKGROUP,
            "database": DATABASE,
            "sql": merge_sql,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    request_token = hashlib.sha256(operation.encode("utf-8")).hexdigest()

    response = athena.start_query_execution(
        QueryString=merge_sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
        ClientRequestToken=request_token,
    )
    query_id = response["QueryExecutionId"]

    # If this invocation fails, the query may still be running/committed.
    # The same token allows a retry of the same execution to rediscover its ID.
    registration = batch_manager(
        {
            "action": "RECORD_QUERY_STARTED",
            "batch_name": batch_name,
            "execution_arn": execution_arn,
            "layer": "gold",
            "merge_query_execution_id": query_id,
        }
    )
    if not registration.get("registered") or registration.get("merge_query_execution_id") != query_id:
        raise RuntimeError("Athena query registration was not confirmed")

    return wait_for_query(query_id, context, is_merge=True)


def lambda_handler(event, context):
    batch_name = event.get("batch_name")
    execution_arn = event.get("execution_arn")

    if batch_name != ALLOWED_BATCH:
        raise ValueError(f"Gold publishing not allowed for {batch_name}")
    if not isinstance(execution_arn, str) or not execution_arn.startswith(
        "arn:aws:states:us-east-1:123456789012:execution:healthcare-lakehouse-ingestion:"
    ):
        raise ValueError("Valid ingestion execution_arn is required")

    ledger = batch_manager({"action": "GET", "batch_name": batch_name})
    if ledger.get("status") != "SILVER_PUBLISHED" or ledger.get("execution_arn") != execution_arn:
        raise ValueError("Gold publishing blocked: batch status or execution owner mismatch")

    existing_id = ledger.get("gold_merge_query_id")
    if existing_id:
        raise RuntimeError(
            f"Gold MERGE already registered ({existing_id}); "
            "use controlled recovery assessment instead of republishing"
        )

    # Preserve the existing check: every affected patient must exist in Gold.
    check_sql = """
    SELECT COUNT(*)
    FROM (
        SELECT DISTINCT patient_id
        FROM healthcare_staging.encounter_batch_001
    ) b
    LEFT JOIN healthcare_gold.patient_utilization g
        ON b.patient_id = g.patient_id
    WHERE g.patient_id IS NULL
    """
    check_id = run_query(check_sql, context)
    missing_patients = get_scalar(check_id)
    if missing_patients > 0:
        raise ValueError(f"{missing_patients} patients missing from Gold")

    merge_sql = """
    MERGE INTO healthcare_gold.patient_utilization AS target
    USING (
        WITH affected_patients AS (
            SELECT DISTINCT patient_id
            FROM healthcare_staging.encounter_batch_001
        ),
        utilization AS (
            SELECT
                e.patient_id,
                COUNT(*) AS total_encounters,
                COUNT_IF(
                    LOWER(e.encounter_class) = 'emergency'
                ) AS emergency_visits,
                COUNT_IF(
                    LOWER(e.encounter_class) = 'inpatient'
                ) AS inpatient_visits,
                CAST(
                    SUM(e.total_claim_cost)
                    AS DECIMAL(38,2)
                ) AS total_claim_cost
            FROM healthcare_silver.encounters e
            INNER JOIN affected_patients a
                ON e.patient_id = a.patient_id
            GROUP BY e.patient_id
        )
        SELECT *
        FROM utilization
    ) AS source
    ON target.patient_id = source.patient_id

    WHEN MATCHED AND (
        target.total_encounters
            IS DISTINCT FROM source.total_encounters
        OR target.emergency_visits
            IS DISTINCT FROM source.emergency_visits
        OR target.inpatient_visits
            IS DISTINCT FROM source.inpatient_visits
        OR target.total_claim_cost
            IS DISTINCT FROM source.total_claim_cost
    )
    THEN UPDATE SET
        total_encounters = source.total_encounters,
        emergency_visits = source.emergency_visits,
        inpatient_visits = source.inpatient_visits,
        total_claim_cost = source.total_claim_cost,
        processed_at = CAST(
            CURRENT_TIMESTAMP AS TIMESTAMP(3)
        )
    """

    merge_id = publish_merge(merge_sql, batch_name, execution_arn, context)
    return {
        "status": "PUBLISHED",
        "layer": "gold",
        "batch_name": batch_name,
        "merge_query_execution_id": merge_id,
    }