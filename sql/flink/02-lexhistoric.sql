-- Lot 1: five Lexhistoric projections. Kafka inputs retain their original topic names.
-- Output is deliberately isolated from the existing ksqlDB output.
-- No key column is declared in the original ksqlDB sources: output keys are null.
-- NormalizeJsonKeys preserves ksqlDB case-insensitive JSON field lookup.
SET 'execution.runtime-mode' = 'streaming';
CREATE TEMPORARY SYSTEM FUNCTION KSQL_JSON AS 'lab.NormalizeJsonKeys';
CREATE TABLE lexhistoric_output (
  `IDTRAN` STRING, `DATETRANSACTION` STRING, `CONTENUISO` STRING,
  `MODEREGLEMENT` INT, `SCA` STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='va.peage.voie.passagesortieva.lexhistoric.flink',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='json',
  'sink.delivery-guarantee'='at-least-once'
);

CREATE TABLE asf_input (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='asf.peage.voie.passagesortieva',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='lexhistoric-flink-asf', 'scan.startup.mode'='earliest-offset',
  'value.format'='raw'
);
CREATE VIEW asf_lexhistoric AS
SELECT kafka_timestamp,
  JSON_VALUE(j, '$.IDPASSAGE') AS `IDTRAN`,
  JSON_VALUE(j, '$.PASSAGESORTIE.DHM') AS `DATETRANSACTION`,
  JSON_VALUE(j, '$.PASSAGESORTIE.PAIEMENT.RETENUS[0].PAN') AS `CONTENUISO`,
  LOWER(JSON_VALUE(j, '$.PASSAGESORTIE.PAIEMENT.RETENUS[0].TYPE')) AS payment_type,
  SUBSTRING(JSON_VALUE(j, '$.PASSAGESORTIE.LOCALISATION.GARE'), 1, 5) AS `SCA`
FROM (SELECT kafka_timestamp, KSQL_JSON(payload) AS j FROM asf_input);

CREATE TABLE esc_input (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='esc.peage.voie.passagesortieva',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='lexhistoric-flink-esc', 'scan.startup.mode'='earliest-offset',
  'value.format'='raw'
);
CREATE VIEW esc_lexhistoric AS
SELECT kafka_timestamp,
  JSON_VALUE(j, '$.IDPASSAGE') AS `IDTRAN`,
  JSON_VALUE(j, '$.PASSAGESORTIE.DHM') AS `DATETRANSACTION`,
  JSON_VALUE(j, '$.PASSAGESORTIE.PAIEMENT.RETENUS[0].PAN') AS `CONTENUISO`,
  LOWER(JSON_VALUE(j, '$.PASSAGESORTIE.PAIEMENT.RETENUS[0].TYPE')) AS payment_type,
  SUBSTRING(JSON_VALUE(j, '$.PASSAGESORTIE.LOCALISATION.GARE'), 1, 5) AS `SCA`
FROM (SELECT kafka_timestamp, KSQL_JSON(payload) AS j FROM esc_input);

CREATE TABLE ars_input (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='ars.peage.voie.passagesortieva',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='lexhistoric-flink-ars', 'scan.startup.mode'='earliest-offset',
  'value.format'='raw'
);
CREATE VIEW ars_lexhistoric AS
SELECT kafka_timestamp,
  JSON_VALUE(j, '$.IDPASSAGE') AS `IDTRAN`,
  JSON_VALUE(j, '$.PASSAGESORTIE.DHM') AS `DATETRANSACTION`,
  JSON_VALUE(j, '$.PASSAGESORTIE.PAIEMENT.RETENUS[0].PAN') AS `CONTENUISO`,
  LOWER(JSON_VALUE(j, '$.PASSAGESORTIE.PAIEMENT.RETENUS[0].TYPE')) AS payment_type,
  SUBSTRING(JSON_VALUE(j, '$.PASSAGESORTIE.LOCALISATION.GARE'), 1, 5) AS `SCA`
FROM (SELECT kafka_timestamp, KSQL_JSON(payload) AS j FROM ars_input);

CREATE TABLE acr_input (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='acr.peage.voie.passagesortieva',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='lexhistoric-flink-acr', 'scan.startup.mode'='earliest-offset',
  'value.format'='raw'
);
CREATE VIEW acr_lexhistoric AS
SELECT kafka_timestamp,
  JSON_VALUE(j, '$.IDPASSAGE') AS `IDTRAN`,
  JSON_VALUE(j, '$.PASSAGESORTIE.DHM') AS `DATETRANSACTION`,
  JSON_VALUE(j, '$.PASSAGESORTIE.PAIEMENT.RETENUS[0].PAN') AS `CONTENUISO`,
  LOWER(JSON_VALUE(j, '$.PASSAGESORTIE.PAIEMENT.RETENUS[0].TYPE')) AS payment_type,
  SUBSTRING(JSON_VALUE(j, '$.PASSAGESORTIE.LOCALISATION.GARE'), 1, 5) AS `SCA`
FROM (SELECT kafka_timestamp, KSQL_JSON(payload) AS j FROM acr_input);

CREATE TABLE cof_input (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='cof.peage.voie.passagesortieva',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='lexhistoric-flink-cof', 'scan.startup.mode'='earliest-offset',
  'value.format'='raw'
);
CREATE VIEW cof_lexhistoric AS
SELECT kafka_timestamp,
  JSON_VALUE(j, '$.IDPASSAGE') AS `IDTRAN`,
  JSON_VALUE(j, '$.PASSAGESORTIE.DHM') AS `DATETRANSACTION`,
  JSON_VALUE(j, '$.PASSAGESORTIE.PAIEMENT.RETENUS[0].PAN') AS `CONTENUISO`,
  LOWER(JSON_VALUE(j, '$.PASSAGESORTIE.PAIEMENT.RETENUS[0].TYPE')) AS payment_type,
  SUBSTRING(JSON_VALUE(j, '$.PASSAGESORTIE.LOCALISATION.GARE'), 1, 5) AS `SCA`
FROM (SELECT kafka_timestamp, KSQL_JSON(payload) AS j FROM cof_input);

INSERT INTO lexhistoric_output
SELECT `IDTRAN`, `DATETRANSACTION`, `CONTENUISO`,
  CASE payment_type
    WHEN 'telepeagelibert' THEN 256 WHEN 'telepeagecardme' THEN 512
    WHEN 'carteaccreditive' THEN 1024 WHEN 'cartemagnetique' THEN 2048
    ELSE 0 END, `SCA`, kafka_timestamp
FROM asf_lexhistoric
WHERE payment_type IN ('telepeagelibert', 'telepeagecardme', 'carteaccreditive', 'cartemagnetique')
UNION ALL
SELECT `IDTRAN`, `DATETRANSACTION`, `CONTENUISO`,
  CASE payment_type
    WHEN 'telepeagelibert' THEN 256 WHEN 'telepeagecardme' THEN 512
    WHEN 'carteaccreditive' THEN 1024 WHEN 'cartemagnetique' THEN 2048
    ELSE 0 END, `SCA`, kafka_timestamp
FROM esc_lexhistoric
WHERE payment_type IN ('telepeagelibert', 'telepeagecardme', 'carteaccreditive', 'cartemagnetique')
UNION ALL
SELECT `IDTRAN`, `DATETRANSACTION`, `CONTENUISO`,
  CASE payment_type
    WHEN 'telepeagelibert' THEN 256 WHEN 'telepeagecardme' THEN 512
    WHEN 'carteaccreditive' THEN 1024 WHEN 'cartemagnetique' THEN 2048
    ELSE 0 END, `SCA`, kafka_timestamp
FROM ars_lexhistoric
WHERE payment_type IN ('telepeagelibert', 'telepeagecardme', 'carteaccreditive', 'cartemagnetique')
UNION ALL
SELECT `IDTRAN`, `DATETRANSACTION`, `CONTENUISO`,
  CASE payment_type
    WHEN 'telepeagelibert' THEN 256 WHEN 'telepeagecardme' THEN 512
    WHEN 'carteaccreditive' THEN 1024 WHEN 'cartemagnetique' THEN 2048
    ELSE 0 END, `SCA`, kafka_timestamp
FROM acr_lexhistoric
WHERE payment_type IN ('telepeagelibert', 'telepeagecardme', 'carteaccreditive', 'cartemagnetique')
UNION ALL
SELECT `IDTRAN`, `DATETRANSACTION`, `CONTENUISO`,
  CASE payment_type
    WHEN 'telepeagelibert' THEN 256 WHEN 'telepeagecardme' THEN 512
    WHEN 'carteaccreditive' THEN 1024 WHEN 'cartemagnetique' THEN 2048
    ELSE 0 END, `SCA`, kafka_timestamp
FROM cof_lexhistoric
WHERE payment_type IN ('telepeagelibert', 'telepeagecardme', 'carteaccreditive', 'cartemagnetique');
