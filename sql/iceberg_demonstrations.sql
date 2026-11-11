-- Read-only Athena queries against deployed Apache Iceberg tables.
-- Snapshot IDs are from this project's observed history; snapshot expiration may invalidate them.

SELECT snapshot_id, committed_at, operation
FROM "healthcare_silver"."encounters$snapshots"
ORDER BY committed_at DESC;

SELECT COUNT(*) AS historical_encounter_count
FROM healthcare_silver.encounters
FOR VERSION AS OF 5009512826829167391;

SELECT COUNT(*) AS current_encounter_count
FROM healthcare_silver.encounters;

SELECT current_data.encounter_id, current_data.patient_id,
       current_data.start_time, current_data.total_claim_cost
FROM healthcare_silver.encounters AS current_data
LEFT JOIN (
    SELECT encounter_id
    FROM healthcare_silver.encounters
    FOR VERSION AS OF 5009512826829167391
) AS historical_data
  ON current_data.encounter_id = historical_data.encounter_id
WHERE historical_data.encounter_id IS NULL
ORDER BY current_data.start_time;

-- Schema evolution statement previously executed successfully:
-- ALTER TABLE healthcare_silver.encounters ADD COLUMNS (data_source STRING);
-- Confirmed afterward: 61,462 rows; all data_source values NULL.
