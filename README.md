# AWS Healthcare Lakehouse — Synthea / Apache Iceberg

A personal AWS proof of concept using **synthetic Synthea healthcare data**. No real patient records are used.

## Architecture
Synthea CSV → Amazon S3 Bronze → Lambda/Step Functions quality and publishing workflows → Athena and Apache Iceberg Silver tables → Gold patient-utilization analytics. AWS Glue catalogs the tables; DynamoDB tracks batch processing; CloudWatch provides monitoring.

## Verified results (November 2026)
| Dataset | Records |
| --- | ---: |
| healthcare_silver.patients | 1,163 |
| healthcare_silver.encounters | 61,462 |
| healthcare_silver.conditions | 38,094 |
| healthcare_gold.patient_utilization | 1,163 |

The counts are point-in-time measurements, not live counters.

## Governance and lineage
- Dataset registry: four datasets
- Column registry: 47 columns, including nullable `data_source`
- Logical lineage registry: six relationships
- Lake Formation LF-Tags were verified for the Silver patients table; other tables' tag coverage remains to be checked.
- The conditions registry was marked *Not Assessed* for quality; other validations were scoped to project tests.

## Apache Iceberg demonstrations
- Four encounter data snapshots
- Time travel to the first snapshot: 61,459 encounters
- Current encounter count: 61,462
- Three newly added encounter IDs identified by historical comparison
- Added `data_source STRING` without rewriting records or creating another data snapshot

## Code and documentation
- [Iceberg queries](sql/iceberg_demonstrations.sql)
- [Governance queries](sql/governance_queries.sql)
- [Governance design and limitations](docs/governance.md)

## Operational reliability
The deployed proof of concept used Step Functions, Lambda, DynamoDB, and CloudWatch. Quality-gate negative testing, successful orchestration, and assessment-only recovery tests were demonstrated. Automated recovery execution is **not** claimed.

## Scope of this repository
The repository now also includes **historical source snapshots** shared during the project build: [batch manager](src/lambda/batch_manager.py), [Silver publisher](src/lambda/silver_publisher.py), [Gold publisher](src/lambda/gold_publisher.py), and [Step Functions definition](orchestration/state_machine.json). AWS account IDs in these snapshots were replaced with placeholders. These files were not re-exported from AWS for this publication, so their correspondence to currently deployed versions and runtime behavior is not verified. The quality-gate and reconciliation Lambda sources are not included. Do not interpret this repository as a deployable infrastructure-as-code solution.

## Security
Do not commit AWS secrets, access keys, real patient records, environment-specific credentials, or runtime logs with sensitive values.
