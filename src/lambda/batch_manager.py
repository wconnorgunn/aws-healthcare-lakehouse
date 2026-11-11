import os
import re
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError


dynamodb = boto3.resource("dynamodb")
sfn = boto3.client("stepfunctions")
athena = boto3.client("athena")
table = dynamodb.Table(os.environ["LEDGER_TABLE"])

WORKGROUP = "healthcare-lakehouse"
ALLOWED_BATCHES = {"encounter_batch_001", "ledger_recovery_test_001", "recovery_execution_test_001" }
MILESTONES = {"CLAIMED", "VALIDATED", "SILVER_PUBLISHED", "GOLD_PUBLISHED", "COMPLETED"}
VALID_STATUSES = MILESTONES | {"FAILED"}
TRANSITIONS = {
    "CLAIMED": {"VALIDATED", "FAILED"},
    "VALIDATED": {"SILVER_PUBLISHED", "FAILED"},
    "SILVER_PUBLISHED": {"GOLD_PUBLISHED", "FAILED"},
    "GOLD_PUBLISHED": {"COMPLETED", "FAILED"},
    "COMPLETED": set(),
    "FAILED": set(),
}
ACTIVE_EXECUTION_STATUSES = {"RUNNING", "PENDING_REDRIVE"}
QUERY_ID_PATTERN = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
CHECKPOINT_MAP = {
    "CLAIMED": "ValidateBatch",
    "VALIDATED": "PublishSilver",
    "SILVER_PUBLISHED": "PublishGold",
    "GOLD_PUBLISHED": "Reconcile",
}


def now_utc():
    return datetime.now(timezone.utc).isoformat()


def get_batch(batch_name):
    response = table.get_item(Key={"batch_name": batch_name}, ConsistentRead=True)
    return response.get("Item")


def conditional_failure(error):
    return error.response["Error"]["Code"] == "ConditionalCheckFailedException"


def inspect_query(query_id):
    if not isinstance(query_id, str) or not QUERY_ID_PATTERN.fullmatch(query_id):
        raise ValueError("Invalid Athena query ID")
    query = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
    status = query["Status"]
    return {
        "query_execution_id": query_id,
        "workgroup": query.get("WorkGroup"),
        "state": status["State"],
        "state_change_reason": status.get("StateChangeReason"),
        "query_type": query.get("StatementType"),
        "submission_time": status["SubmissionDateTime"].isoformat() if status.get("SubmissionDateTime") else None,
        "completion_time": status["CompletionDateTime"].isoformat() if status.get("CompletionDateTime") else None,
    }


def claim_batch(batch_name, execution_arn):
    if not execution_arn:
        raise ValueError("execution_arn is required")
    timestamp = now_utc()
    item = {
        "batch_name": batch_name,
        "status": "CLAIMED",
        "last_successful_milestone": "CLAIMED",
        "execution_arn": execution_arn,
        "started_at": timestamp,
        "updated_at": timestamp,
    }
    try:
        table.put_item(Item=item, ConditionExpression="attribute_not_exists(batch_name)")
    except ClientError as error:
        if conditional_failure(error):
            raise ValueError("Batch already claimed") from error
        raise
    return {
        "batch_name": batch_name,
        "status": "CLAIMED",
        "last_successful_milestone": "CLAIMED",
        "execution_arn": execution_arn,
    }


def update_batch(batch_name, new_status, execution_arn, failure_reason=None, merge_query_execution_id=None):
    if new_status not in VALID_STATUSES:
        raise ValueError("Invalid status")
    if not execution_arn:
        raise ValueError("execution_arn is required")
    current = get_batch(batch_name)
    if not current:
        raise ValueError("Batch does not exist")
    old_status = current["status"]
    if new_status not in TRANSITIONS.get(old_status, set()):
        raise ValueError(f"Invalid transition: {old_status} -> {new_status}")

    timestamp = now_utc()
    values = {":new": new_status, ":old": old_status, ":owner": execution_arn, ":now": timestamp}
    names = {"#s": "status"}
    expression = "SET #s = :new, updated_at = :now"

    if new_status == "FAILED":
        if not isinstance(failure_reason, str) or not failure_reason.strip():
            raise ValueError("failure_reason is required for FAILED")
        expression += ", failed_at = :failed_at, failure_reason = :reason"
        values[":failed_at"] = timestamp
        values[":reason"] = failure_reason.strip()[:1000]
    else:
        expression += ", last_successful_milestone = :milestone"
        values[":milestone"] = new_status
        if new_status in {"SILVER_PUBLISHED", "GOLD_PUBLISHED"}:
            if not merge_query_execution_id:
                raise ValueError("merge_query_execution_id is required")
            query = inspect_query(merge_query_execution_id)
            if query["workgroup"] != WORKGROUP:
                raise ValueError("Athena query belongs to an unexpected workgroup")
            if query["state"] != "SUCCEEDED":
                raise ValueError("Athena MERGE query has not succeeded")
            attribute = "silver_merge_query_id" if new_status == "SILVER_PUBLISHED" else "gold_merge_query_id"
            names["#qid"] = attribute
            expression += ", #qid = :query_id"
            values[":query_id"] = merge_query_execution_id
        elif merge_query_execution_id is not None:
            raise ValueError("Query ID is only accepted for Silver or Gold publishing")
        if new_status == "COMPLETED":
            expression += ", completed_at = :completed_at"
            values[":completed_at"] = timestamp

    try:
        response = table.update_item(
            Key={"batch_name": batch_name},
            UpdateExpression=expression,
            ConditionExpression=(
                "#s = :old AND execution_arn = :owner"
                + (" AND (attribute_not_exists(#qid) OR #qid = :query_id)"
                   if "#qid" in names else "")
            ),
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
            ReturnValues="ALL_NEW",
        )
    except ClientError as error:
        if conditional_failure(error):
            raise ValueError("Concurrent update or execution ownership mismatch") from error
        raise

    item = response["Attributes"]
    result = {
        "batch_name": batch_name,
        "status": item["status"],
        "last_successful_milestone": item.get("last_successful_milestone"),
        "execution_arn": execution_arn,
    }
    for attribute in ("silver_merge_query_id", "gold_merge_query_id"):
        if item.get(attribute):
            result[attribute] = item[attribute]
    return result



def record_query_started(batch_name, execution_arn, layer, query_id):
    """Register a submitted MERGE without advancing the batch milestone."""
    if not isinstance(execution_arn, str) or not execution_arn.strip():
        raise ValueError("execution_arn is required")
    layer_config = {
        "silver": ("VALIDATED", "silver_merge_query_id", "healthcare_silver.encounters"),
        "gold": ("SILVER_PUBLISHED", "gold_merge_query_id", "healthcare_gold.patient_utilization"),
    }
    if layer not in layer_config:
        raise ValueError("layer must be silver or gold")
    required_status, attribute, expected_target = layer_config[layer]
    if not isinstance(query_id, str) or not QUERY_ID_PATTERN.fullmatch(query_id):
        raise ValueError("Invalid Athena query ID")

    # Verify that the query is the expected write, not merely any Athena query.
    execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
    if execution.get("WorkGroup") != WORKGROUP:
        raise ValueError("Athena query belongs to an unexpected workgroup")
    sql = execution.get("Query", "")
    normalized = re.sub(r"\s+", " ", sql).strip().lower()
    if not re.match(
        r"^merge\s+into\s+" + re.escape(expected_target.lower()) + r"(?:\s|$)",
        normalized,
    ):
        raise ValueError("Athena query is not the expected layer MERGE")
    if execution.get("Status", {}).get("State") in {"FAILED", "CANCELLED"}:
        raise ValueError("Cannot register a failed or cancelled query as started")

    timestamp = now_utc()
    try:
        response = table.update_item(
            Key={"batch_name": batch_name},
            UpdateExpression="SET #qid = :qid, updated_at = :now",
            ConditionExpression=(
                "attribute_exists(batch_name) AND #s = :required "
                "AND execution_arn = :owner "
                "AND (attribute_not_exists(#qid) OR #qid = :qid)"
            ),
            ExpressionAttributeNames={"#s": "status", "#qid": attribute},
            ExpressionAttributeValues={
                ":required": required_status,
                ":owner": execution_arn,
                ":qid": query_id,
                ":now": timestamp,
            },
            ReturnValues="ALL_NEW",
        )
    except ClientError as error:
        if conditional_failure(error):
            raise ValueError(
                "Query registration rejected: batch status, execution owner, "
                "or previously recorded query ID does not match"
            ) from error
        raise
    item = response["Attributes"]
    return {
        "batch_name": batch_name,
        "status": item["status"],
        "layer": layer,
        "merge_query_execution_id": query_id,
        "registered": True,
    }


def check_recovery(batch_name):
    item = get_batch(batch_name)
    if not item:
        return {"batch_name": batch_name, "recovery_eligible": False, "decision": "NEW_BATCH", "reason": "Use normal batch claiming."}
    status = item.get("status", "UNKNOWN")
    result = {
        "batch_name": batch_name,
        "ledger_status": status,
        "last_successful_milestone": item.get("last_successful_milestone"),
        "recovery_eligible": False,
    }
    if status == "COMPLETED":
        result.update(decision="BLOCKED_COMPLETED", reason="Completed batches cannot be recovered automatically.")
        return result
    execution_arn = item.get("execution_arn")
    if not execution_arn:
        result.update(decision="MANUAL_REVIEW", reason="Missing execution ARN.")
        return result
    try:
        execution_status = sfn.describe_execution(executionArn=execution_arn)["status"]
    except ClientError as error:
        result.update(decision="MANUAL_REVIEW", reason="Could not verify Step Functions: " + error.response["Error"]["Code"])
        return result
    result["execution_status"] = execution_status
    if execution_status in ACTIVE_EXECUTION_STATUSES:
        result.update(decision="BLOCKED_ACTIVE", reason="Previous execution is active or pending redrive.")
        return result

    query_results = {}
    missing_queries = []
    for layer, attribute in (("silver", "silver_merge_query_id"), ("gold", "gold_merge_query_id")):
        query_id = item.get(attribute)
        if not query_id:
            missing_queries.append(layer)
            continue
        try:
            query_results[layer] = inspect_query(query_id)
        except (ClientError, ValueError) as error:
            query_results[layer] = {"query_execution_id": query_id, "state": "UNKNOWN", "error": type(error).__name__}
    result["athena_queries"] = query_results
    result["missing_query_ids"] = missing_queries
    if any(q["state"] in {"RUNNING", "QUEUED", "UNKNOWN"} for q in query_results.values()):
        result.update(decision="BLOCKED_QUERY_UNVERIFIED", reason="An Athena query is active or could not be verified.")
        return result
    result.update(decision="MANUAL_REVIEW", reason="Recorded queries were inspected. Missing queries, unrecorded writes, and Iceberg commit state must be verified before recovery.")
    return result


def assess_recovery(batch_name):
    """Read-only candidate checkpoint assessment. Never authorizes replay."""
    item = get_batch(batch_name)
    if not item:
        return {
            "batch_name": batch_name,
            "decision": "NEW_BATCH",
            "recovery_eligible": False,
            "recommended_checkpoint": None,
            "reason": "No existing ledger record.",
        }

    status = item.get("status")
    milestone = item.get("last_successful_milestone")
    result = {
        "batch_name": batch_name,
        "ledger_status": status,
        "last_successful_milestone": milestone,
        "recovery_eligible": False,
        "recommended_checkpoint": None,
        "checks": {},
    }
    if status == "COMPLETED":
        result.update(decision="BLOCKED_COMPLETED", reason="Batch completed successfully.")
        return result

    execution_arn = item.get("execution_arn")
    if not execution_arn:
        result.update(decision="MANUAL_REVIEW", reason="Missing execution ARN.")
        return result
    try:
        execution_status = sfn.describe_execution(executionArn=execution_arn)["status"]
        result["checks"]["step_functions"] = {"status": execution_status, "verified": True}
    except ClientError as error:
        result.update(decision="MANUAL_REVIEW", reason="Unable to inspect previous execution: " + error.response["Error"]["Code"])
        return result

    if execution_status in ACTIVE_EXECUTION_STATUSES:
        result.update(decision="BLOCKED_ACTIVE", reason="Previous execution is still active.")
        return result

    for layer, attribute in (("silver", "silver_merge_query_id"), ("gold", "gold_merge_query_id")):
        query_id = item.get(attribute)
        if not query_id:
            result["checks"][layer] = {"verified": False, "reason": "No recorded query ID."}
            continue
        try:
            query = inspect_query(query_id)
            result["checks"][layer] = {
                "verified": query["state"] == "SUCCEEDED" and query["workgroup"] == WORKGROUP,
                "query_execution_id": query_id,
                "state": query["state"],
                "workgroup": query["workgroup"],
            }
        except (ClientError, ValueError) as error:
            result["checks"][layer] = {"verified": False, "reason": type(error).__name__}

    if milestone not in CHECKPOINT_MAP:
        result.update(decision="MANUAL_REVIEW", reason="Unknown recovery milestone.")
        return result
    result["recommended_checkpoint"] = CHECKPOINT_MAP[milestone]
    result.update(
        decision="ASSESSMENT_ONLY",
        reason="Candidate checkpoint identified. Iceberg commits and unrecorded Athena queries require verification before recovery can be authorized.",
    )
    return result


def lambda_handler(event, context):
    action = event.get("action")
    batch_name = event.get("batch_name")
    if batch_name not in ALLOWED_BATCHES:
        raise ValueError("Batch is not authorized")
    if action == "CLAIM":
        return claim_batch(batch_name, event.get("execution_arn"))
    if action == "GET":
        return get_batch(batch_name) or {}
    if action == "UPDATE":
        return update_batch(
            batch_name=batch_name,
            new_status=event.get("status"),
            execution_arn=event.get("execution_arn"),
            failure_reason=event.get("failure_reason"),
            merge_query_execution_id=event.get("merge_query_execution_id"),
        )
    if action == "RECORD_QUERY_STARTED":
        return record_query_started(
            batch_name=batch_name,
            execution_arn=event.get("execution_arn"),
            layer=event.get("layer"),
            query_id=event.get("merge_query_execution_id"),
        )
    if action == "ASSESS_RECOVERY":
        return assess_recovery(batch_name)
    if action == "CHECK_RECOVERY":
        return check_recovery(batch_name)
    raise ValueError("Unsupported action. Use CLAIM, GET, UPDATE, RECORD_QUERY_STARTED, ASSESS_RECOVERY, or CHECK_RECOVERY")