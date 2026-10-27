-- Lot 2: nine append-only consolidation branches; no ARS entree branch in the journal.
-- ProjectKsqlJson reproduces SELECT * schema serialization, not a raw JSON passthrough.
CREATE TEMPORARY SYSTEM FUNCTION KSQL_PROJECT AS 'lab.ProjectKsqlJson';

CREATE TABLE entree_output (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='va.peage.backoffice.conformite.entree.conforme.flink',
  'properties.bootstrap.servers'='${KAFKA_BOOTSTRAP}',
  'properties.security.protocol'='${KAFKA_SECURITY_PROTOCOL}',
  'properties.sasl.mechanism'='${KAFKA_SASL_MECHANISM}',
  'properties.sasl.jaas.config'='${KAFKA_JAAS}',
  'format'='raw',
  'sink.delivery-guarantee'='at-least-once'
);

CREATE TABLE sortie_output (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='va.peage.backoffice.conformite.sortie.conforme.flink',
  'properties.bootstrap.servers'='${KAFKA_BOOTSTRAP}',
  'properties.security.protocol'='${KAFKA_SECURITY_PROTOCOL}',
  'properties.sasl.mechanism'='${KAFKA_SASL_MECHANISM}',
  'properties.sasl.jaas.config'='${KAFKA_JAAS}',
  'format'='raw',
  'sink.delivery-guarantee'='at-least-once'
);

CREATE TABLE entree_asf (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='asf.peage.backoffice.conformite.entree.conforme',
  'properties.bootstrap.servers'='${KAFKA_BOOTSTRAP}',
  'properties.security.protocol'='${KAFKA_SECURITY_PROTOCOL}',
  'properties.sasl.mechanism'='${KAFKA_SASL_MECHANISM}',
  'properties.sasl.jaas.config'='${KAFKA_JAAS}',
  'properties.group.id'='conformite-flink-entree-asf',
  'scan.startup.mode'='earliest-offset', 'format'='raw'
);

CREATE TABLE entree_esc (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='esc.peage.backoffice.conformite.entree.conforme',
  'properties.bootstrap.servers'='${KAFKA_BOOTSTRAP}',
  'properties.security.protocol'='${KAFKA_SECURITY_PROTOCOL}',
  'properties.sasl.mechanism'='${KAFKA_SASL_MECHANISM}',
  'properties.sasl.jaas.config'='${KAFKA_JAAS}',
  'properties.group.id'='conformite-flink-entree-esc',
  'scan.startup.mode'='earliest-offset', 'format'='raw'
);

CREATE TABLE entree_acr (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='acr.peage.backoffice.conformite.entree.conforme',
  'properties.bootstrap.servers'='${KAFKA_BOOTSTRAP}',
  'properties.security.protocol'='${KAFKA_SECURITY_PROTOCOL}',
  'properties.sasl.mechanism'='${KAFKA_SASL_MECHANISM}',
  'properties.sasl.jaas.config'='${KAFKA_JAAS}',
  'properties.group.id'='conformite-flink-entree-acr',
  'scan.startup.mode'='earliest-offset', 'format'='raw'
);

CREATE TABLE entree_cof (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='cof.peage.backoffice.conformite.entree.conforme',
  'properties.bootstrap.servers'='${KAFKA_BOOTSTRAP}',
  'properties.security.protocol'='${KAFKA_SECURITY_PROTOCOL}',
  'properties.sasl.mechanism'='${KAFKA_SASL_MECHANISM}',
  'properties.sasl.jaas.config'='${KAFKA_JAAS}',
  'properties.group.id'='conformite-flink-entree-cof',
  'scan.startup.mode'='earliest-offset', 'format'='raw'
);

CREATE TABLE sortie_asf (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='asf.peage.backoffice.conformite.sortie.conforme',
  'properties.bootstrap.servers'='${KAFKA_BOOTSTRAP}',
  'properties.security.protocol'='${KAFKA_SECURITY_PROTOCOL}',
  'properties.sasl.mechanism'='${KAFKA_SASL_MECHANISM}',
  'properties.sasl.jaas.config'='${KAFKA_JAAS}',
  'properties.group.id'='conformite-flink-sortie-asf',
  'scan.startup.mode'='earliest-offset', 'format'='raw'
);

CREATE TABLE sortie_esc (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='esc.peage.backoffice.conformite.sortie.conforme',
  'properties.bootstrap.servers'='${KAFKA_BOOTSTRAP}',
  'properties.security.protocol'='${KAFKA_SECURITY_PROTOCOL}',
  'properties.sasl.mechanism'='${KAFKA_SASL_MECHANISM}',
  'properties.sasl.jaas.config'='${KAFKA_JAAS}',
  'properties.group.id'='conformite-flink-sortie-esc',
  'scan.startup.mode'='earliest-offset', 'format'='raw'
);

CREATE TABLE sortie_ars (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='ars.peage.backoffice.conformite.sortie.conforme',
  'properties.bootstrap.servers'='${KAFKA_BOOTSTRAP}',
  'properties.security.protocol'='${KAFKA_SECURITY_PROTOCOL}',
  'properties.sasl.mechanism'='${KAFKA_SASL_MECHANISM}',
  'properties.sasl.jaas.config'='${KAFKA_JAAS}',
  'properties.group.id'='conformite-flink-sortie-ars',
  'scan.startup.mode'='earliest-offset', 'format'='raw'
);

CREATE TABLE sortie_acr (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='acr.peage.backoffice.conformite.sortie.conforme',
  'properties.bootstrap.servers'='${KAFKA_BOOTSTRAP}',
  'properties.security.protocol'='${KAFKA_SECURITY_PROTOCOL}',
  'properties.sasl.mechanism'='${KAFKA_SASL_MECHANISM}',
  'properties.sasl.jaas.config'='${KAFKA_JAAS}',
  'properties.group.id'='conformite-flink-sortie-acr',
  'scan.startup.mode'='earliest-offset', 'format'='raw'
);

CREATE TABLE sortie_cof (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='cof.peage.backoffice.conformite.sortie.conforme',
  'properties.bootstrap.servers'='${KAFKA_BOOTSTRAP}',
  'properties.security.protocol'='${KAFKA_SECURITY_PROTOCOL}',
  'properties.sasl.mechanism'='${KAFKA_SASL_MECHANISM}',
  'properties.sasl.jaas.config'='${KAFKA_JAAS}',
  'properties.group.id'='conformite-flink-sortie-cof',
  'scan.startup.mode'='earliest-offset', 'format'='raw'
);

EXECUTE STATEMENT SET
BEGIN
INSERT INTO entree_output
SELECT KSQL_PROJECT(payload, 'entree'), kafka_timestamp
FROM entree_asf
UNION ALL
SELECT KSQL_PROJECT(payload, 'entree'), kafka_timestamp
FROM entree_esc
UNION ALL
SELECT KSQL_PROJECT(payload, 'entree'), kafka_timestamp
FROM entree_acr
UNION ALL
SELECT KSQL_PROJECT(payload, 'entree'), kafka_timestamp
FROM entree_cof;
INSERT INTO sortie_output
SELECT KSQL_PROJECT(payload, 'sortie'), kafka_timestamp
FROM sortie_asf
UNION ALL
SELECT KSQL_PROJECT(payload, 'sortie'), kafka_timestamp
FROM sortie_esc
UNION ALL
SELECT KSQL_PROJECT(payload, 'sortie'), kafka_timestamp
FROM sortie_ars
UNION ALL
SELECT KSQL_PROJECT(payload, 'sortie'), kafka_timestamp
FROM sortie_acr
UNION ALL
SELECT KSQL_PROJECT(payload, 'sortie'), kafka_timestamp
FROM sortie_cof;
END;
