-- Lot 2: nine append-only consolidation branches; no ARS entree branch in the journal.
-- Branches of one direction share schema and sink: one multi-topic source per direction
-- replaces the per-network sources and UNION ALL. Per-partition order is unchanged.
-- ProjectKsqlJson reproduces SELECT * schema serialization, not a raw JSON passthrough.
CREATE TEMPORARY SYSTEM FUNCTION KSQL_PROJECT AS 'lab.ProjectKsqlJson';

CREATE TABLE entree_input (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='asf.peage.backoffice.conformite.entree.conforme;esc.peage.backoffice.conformite.entree.conforme;acr.peage.backoffice.conformite.entree.conforme;cof.peage.backoffice.conformite.entree.conforme',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='conformite-flink-entree',
  'scan.startup.mode'='earliest-offset', 'format'='raw'
);

CREATE TABLE entree_output (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='va.peage.backoffice.conformite.entree.conforme.flink',
  'properties.bootstrap.servers'='kafka:9092', 'format'='raw',
  'sink.delivery-guarantee'='at-least-once'
);

CREATE TABLE sortie_input (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='asf.peage.backoffice.conformite.sortie.conforme;esc.peage.backoffice.conformite.sortie.conforme;ars.peage.backoffice.conformite.sortie.conforme;acr.peage.backoffice.conformite.sortie.conforme;cof.peage.backoffice.conformite.sortie.conforme',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='conformite-flink-sortie',
  'scan.startup.mode'='earliest-offset', 'format'='raw'
);

CREATE TABLE sortie_output (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='va.peage.backoffice.conformite.sortie.conforme.flink',
  'properties.bootstrap.servers'='kafka:9092', 'format'='raw',
  'sink.delivery-guarantee'='at-least-once'
);

EXECUTE STATEMENT SET
BEGIN
INSERT INTO entree_output
SELECT KSQL_PROJECT(payload, 'entree'), kafka_timestamp FROM entree_input;
INSERT INTO sortie_output
SELECT KSQL_PROJECT(payload, 'sortie'), kafka_timestamp FROM sortie_input;
END;
