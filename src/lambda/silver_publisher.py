"""Publish validated synthetic encounter batches into the Silver Iceberg layer.

Runs Athena queries and coordinates batch status through the batch manager.
An unresolved MERGE must be inspected before retrying because it may commit."""

import boto3
import hashlib
import json
import os
import time

athena = boto3.client("athena")
lambda_client = boto3.client("lambda")

WORKGROUP = "healthcare-lakehouse"
DATABASE = "healthcare_silver"

# Explicit batch-to-staging-table mapping.
# Never construct SQL table names directly from untrusted event values.
ALLOWED_BATCHES = {
    "encounter_batch_001": "healthcare_staging.encounter_batch_001",
    "phase107_integration_001": "healthcare_staging.phase107_integration_001",
    "phase107_integration_002": "healthcare_staging.phase107_integration_002",
}

BATCH_MANAGER_FUNCTION = os.environ.get(
    "BATCH_MANAGER_FUNCTION",
    "healthcare-lakehouse-batch-manager"
)

EXECUTION_ARN_PREFIX = (
    "arn:aws:states:us-east-1:123456789012:"
    "execution:healthcare-lakehouse-ingestion:"
)


def batch_manager(payload):
    """Invoke the batch manager synchronously and validate its response."""
    response = lambda_client.invoke(
        FunctionName=BATCH_MANAGER_FUNCTION,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )

    body = json.loads(response["Payload"].read())

    if response.get("FunctionError"):
        raise RuntimeError(
            f"Batch manager rejected request: {body}"
        )

    if response.get("StatusCode") != 200 or not isinstance(body, dict):
        raise RuntimeError(
            f"Unexpected batch manager response: {body}"
        )

    return body


def wait_for_query(query_id, context, *, is_merge=False):
    """Poll Athena while respecting Lambda time remaining; preserve unresolved MERGE IDs."""
    while context.get_remaining_time_in_millis() > 10000:
        execution = athena.get_query_execution(
            QueryExecutionId=query_id
        )

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

    # A MERGE may already have committed.
    # Never assume cancellation rolled it back.
    # Leave the registered ID for explicit recovery assessment.
    if not is_merge:
        athena.stop_query_execution(
            QueryExecutionId=query_id
        )

    raise TimeoutError(
        f"Athena {'MERGE' if is_merge else 'query'} "
        f"still unresolved: {query_id}"
    )


def run_query(sql, context):
    """Submit an Athena query in the configured Silver workgroup."""
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            "Database": DATABASE
        },
        WorkGroup=WORKGROUP,
    )

    return wait_for_query(
        response["QueryExecutionId"],
        context
    )


def get_scalar(query_id):
    """Read a single scalar value from an Athena result."""
    response = athena.get_query_results(
        QueryExecutionId=query_id
    )

    rows = response["ResultSet"]["Rows"]

    if len(rows) != 2:
        raise RuntimeError(
            "Unexpected Athena result"
        )

    return int(
        rows[1]["Data"][0]["VarCharValue"]
    )


def publish_merge(
    merge_sql,
    batch_name,
    execution_arn,
    context
):
    # Deterministic for this exact execution and SQL.
    # A retry with identical parameters receives the
    # original Athena QueryExecutionId.
    #
    # Including batch_name, execution_arn and SQL
    # prevents the integration batch from sharing
    # the original batch's request token.

    operation = json.dumps(
        {
            "batch_name": batch_name,
            "execution_arn": execution_arn,
            "layer": "silver",
            "workgroup": WORKGROUP,
            "database": DATABASE,
            "sql": merge_sql,
        },
        sort_keys=True,
        separators=(",", ":"),
    )

    request_token = hashlib.sha256(
        operation.encode("utf-8")
    ).hexdigest()

    response = athena.start_query_execution(
        QueryString=merge_sql,
        QueryExecutionContext={
            "Database": DATABASE
        },
        WorkGroup=WORKGROUP,
        ClientRequestToken=request_token,
    )

    query_id = response["QueryExecutionId"]

    # Register immediately after Athena accepts the MERGE.
    # If registration fails, the query may still be
    # running or may already have committed.
    #
    # A retry using the same token can rediscover
    # the original query ID.

    registration = batch_manager(
        {
            "action": "RECORD_QUERY_STARTED",
            "batch_name": batch_name,
            "execution_arn": execution_arn,
            "layer": "silver",
            "merge_query_execution_id": query_id,
        }
    )

    if (
        not registration.get("registered")
        or registration.get("merge_query_execution_id") != query_id
    ):
        raise RuntimeError(
            "Athena query registration was not confirmed"
        )

    return wait_for_query(
        query_id,
        context,
        is_merge=True
    )


def lambda_handler(event, context):
    """Handle the lambda handler operation in this pipeline."""
    batch_name = event.get("batch_name")
    execution_arn = event.get("execution_arn")

    # Only explicitly configured batches may publish.
    if batch_name not in ALLOWED_BATCHES:
        raise ValueError(
            f"Publishing not allowed for batch: {batch_name}"
        )

    # Require a valid-looking execution ARN from
    # the expected ingestion state machine.
    if (
        not isinstance(execution_arn, str)
        or not execution_arn.startswith(
            EXECUTION_ARN_PREFIX
        )
        or len(execution_arn) <= len(EXECUTION_ARN_PREFIX)
    ):
        raise ValueError(
            "Valid ingestion execution_arn is required"
        )

    # Verify the batch has passed validation and
    # is owned by this execution.
    ledger = batch_manager(
        {
            "action": "GET",
            "batch_name": batch_name
        }
    )

    if (
        ledger.get("status") != "VALIDATED"
        or ledger.get("execution_arn") != execution_arn
    ):
        raise ValueError(
            "Silver publishing blocked: "
            "batch status or execution owner mismatch"
        )

    # Prevent a second MERGE when one has already
    # been registered in the ingestion ledger.
    existing_id = ledger.get("silver_merge_query_id")

    if existing_id:
        raise RuntimeError(
            f"Silver MERGE already registered ({existing_id}); "
            "use controlled recovery assessment "
            "instead of republishing"
        )

    # Select the staging table from the allowlist.
    source = ALLOWED_BATCHES[batch_name]

    # --------------------------------------------------
    # Step 1: Validate staging records
    # --------------------------------------------------

    validation_sql = f"""
    SELECT COUNT(*)
    FROM {source} b
    WHERE
        b.encounter_id IS NULL
        OR TRIM(b.encounter_id) = ''
        OR b.patient_id IS NULL
        OR b.start_time IS NULL
        OR b.end_time IS NULL
        OR b.end_time < b.start_time
        OR b.total_claim_cost IS NULL
        OR b.total_claim_cost < 0
        OR NOT EXISTS (
            SELECT 1
            FROM healthcare_silver.patients p
            WHERE p.patient_id = b.patient_id
        )
    """

    check_id = run_query(
        validation_sql,
        context
    )

    invalid_count = get_scalar(check_id)

    if invalid_count > 0:
        raise ValueError(
            f"Publishing blocked: "
            f"{invalid_count} invalid records"
        )

    # --------------------------------------------------
    # Step 2: Check duplicate encounter IDs
    # --------------------------------------------------

    duplicate_sql = f"""
    SELECT
        COUNT(*) - COUNT(DISTINCT encounter_id)
    FROM {source}
    """

    duplicate_id = run_query(
        duplicate_sql,
        context
    )

    duplicates = get_scalar(duplicate_id)

    if duplicates > 0:
        raise ValueError(
            f"Publishing blocked: "
            f"{duplicates} duplicate IDs"
        )

    # --------------------------------------------------
    # Step 3: Verify the batch is not empty
    # --------------------------------------------------

    count_sql = f"""
    SELECT COUNT(*)
    FROM {source}
    """

    count_id = run_query(
        count_sql,
        context
    )

    batch_count = get_scalar(count_id)

    if batch_count == 0:
        raise ValueError(
            "Publishing blocked: empty batch"
        )

    # --------------------------------------------------
    # Step 4: Build the Iceberg MERGE
    # --------------------------------------------------

    merge_sql = f"""
    MERGE INTO healthcare_silver.encounters AS target
    USING {source} AS source
    ON target.encounter_id = source.encounter_id

    WHEN MATCHED AND (
        target.total_claim_cost
            IS DISTINCT FROM source.total_claim_cost
    )
    THEN UPDATE SET
        total_claim_cost = source.total_claim_cost,
        processed_at = CAST(
            CURRENT_TIMESTAMP AS TIMESTAMP(3)
        )

    WHEN NOT MATCHED THEN INSERT (
        encounter_id,
        patient_id,
        start_time,
        end_time,
        encounter_class,
        encounter_code,
        description,
        organization_id,
        provider_id,
        payer_id,
        base_encounter_cost,
        total_claim_cost,
        payer_coverage,
        processed_at
    )
    VALUES (
        source.encounter_id,
        source.patient_id,
        source.start_time,
        source.end_time,
        source.encounter_class,
        source.encounter_code,
        source.description,
        source.organization_id,
        source.provider_id,
        source.payer_id,
        source.base_encounter_cost,
        source.total_claim_cost,
        source.payer_coverage,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
    )
    """

    # --------------------------------------------------
    # Step 5: Submit, register, and monitor the MERGE
    # --------------------------------------------------

    merge_id = publish_merge(
        merge_sql,
        batch_name,
        execution_arn,
        context
    )

    return {
        "status": "PUBLISHED",
        "batch_name": batch_name,
        "validated_records": batch_count,
        "merge_query_execution_id": merge_id,
    }