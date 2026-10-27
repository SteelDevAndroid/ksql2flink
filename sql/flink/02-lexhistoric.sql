-- Lot 1: five Lexhistoric projections in a single pipeline.
-- The five ksqlDB queries share schema, mapping and sink: one multi-topic source replaces
-- five sources, five views and a five-way UNION ALL. Per-partition order is unchanged.
-- No key column is declared in the original ksqlDB sources: output keys are null.
-- NormalizeJsonKeys preserves ksqlDB case-insensitive JSON field lookup.
SET 'execution.runtime-mode' = 'streaming';
CREATE TEMPORARY SYSTEM FUNCTION KSQL_JSON AS 'lab.NormalizeJsonKeys';

CREATE TABLE passagesortieva_input (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka',
  'topic'='asf.peage.voie.passagesortieva;esc.peage.voie.passagesortieva;ars.peage.voie.passagesortieva;acr.peage.voie.passagesortieva;cof.peage.voie.passagesortieva',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='lexhistoric-flink', 'scan.startup.mode'='earliest-offset',
  'value.format'='raw'
);

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

CREATE VIEW lexhistoric AS
SELECT kafka_timestamp,
  JSON_VALUE(j, '$.IDPASSAGE') AS `IDTRAN`,
  JSON_VALUE(j, '$.PASSAGESORTIE.DHM') AS `DATETRANSACTION`,
  JSON_VALUE(j, '$.PASSAGESORTIE.PAIEMENT.RETENUS[0].PAN') AS `CONTENUISO`,
  LOWER(JSON_VALUE(j, '$.PASSAGESORTIE.PAIEMENT.RETENUS[0].TYPE')) AS payment_type,
  SUBSTRING(JSON_VALUE(j, '$.PASSAGESORTIE.LOCALISATION.GARE'), 1, 5) AS `SCA`
FROM (SELECT kafka_timestamp, KSQL_JSON(payload) AS j FROM passagesortieva_input);

INSERT INTO lexhistoric_output
SELECT `IDTRAN`, `DATETRANSACTION`, `CONTENUISO`,
  CASE payment_type
    WHEN 'telepeagelibert' THEN 256 WHEN 'telepeagecardme' THEN 512
    WHEN 'carteaccreditive' THEN 1024 WHEN 'cartemagnetique' THEN 2048
    ELSE 0 END,
  `SCA`, kafka_timestamp
FROM lexhistoric
WHERE payment_type IN ('telepeagelibert', 'telepeagecardme', 'carteaccreditive', 'cartemagnetique');
