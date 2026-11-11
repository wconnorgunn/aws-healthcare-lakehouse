-- Athena metadata queries; requires governance CSV tables to be registered.
SELECT dataset_name, business_owner, data_steward,
       data_classification, quality_status, record_count
FROM healthcare_governance.dataset_registry
ORDER BY dataset_name;

SELECT dataset_name, COUNT(*) AS documented_columns
FROM healthcare_governance.column_registry
GROUP BY dataset_name
ORDER BY dataset_name;

SELECT source_dataset, target_dataset, source_layer, target_layer,
       transformation_description, processing_service
FROM healthcare_governance.data_lineage
ORDER BY target_dataset, source_dataset;
