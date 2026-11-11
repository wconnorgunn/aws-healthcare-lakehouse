# Governance and lineage

The governance layer uses three Athena external CSV tables in `healthcare_governance`:

1. `dataset_registry` — dataset names, business owners, stewards, domain, source, layer, classification, quality status, and record counts.
2. `column_registry` — 47 column names, data types, business definitions, sensitivity classes, owner and steward.
3. `data_lineage` — six logical source-to-target relationships.

## Logical lineage
- Synthea patients.csv → Bronze S3 → healthcare_silver.patients
- Synthea encounters.csv → Bronze S3 → healthcare_silver.encounters
- Synthea conditions.csv → Bronze S3 → healthcare_silver.conditions
- healthcare_silver.patients → healthcare_gold.patient_utilization
- healthcare_silver.encounters → healthcare_gold.patient_utilization
- healthcare_silver.conditions → healthcare_gold.patient_utilization

This is manually curated **logical lineage**, not automatically captured execution lineage.

## Classification
The classification vocabulary is Public, Internal, Confidential and Restricted. All four dataset-level registry entries are Restricted because they contain patient-level synthetic healthcare data. A table-level Restricted tag and column overrides were verified on the Silver patients table; other LF-Tag assignments still require validation.

## Important limitations
- CSV governance registries are manually maintained; counts may become stale.
- Documented classifications do not themselves enforce permissions.
- The conditions dataset's registry quality status was Not Assessed.
- The Silver encounters `data_source` column was added through Iceberg schema evolution and remains unpopulated.
