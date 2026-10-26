SET 'execution.runtime-mode' = 'streaming';
CREATE TABLE lab_input (
  `ID` STRING, `PAYMENT_TYPE` STRING, `AMOUNT` DOUBLE
) WITH (
  'connector' = 'kafka', 'topic' = 'lab.input',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'lab-flink-smoke',
  'scan.startup.mode' = 'earliest-offset', 'format' = 'json'
);
CREATE TABLE lab_output (
  `ID` STRING, `AMOUNT` DOUBLE, `MODEREGLEMENT` INT
) WITH (
  'connector' = 'kafka', 'topic' = 'lab.output.flink',
  'properties.bootstrap.servers' = 'kafka:9092', 'format' = 'json',
  'sink.delivery-guarantee' = 'at-least-once'
);
INSERT INTO lab_output
SELECT `ID`, `AMOUNT`, CASE LOWER(`PAYMENT_TYPE`)
  WHEN 'telepeagelibert' THEN 256
  WHEN 'telepeagecardme' THEN 512
  WHEN 'carteaccreditive' THEN 1024
  WHEN 'cartemagnetique' THEN 2048
  ELSE 0 END
FROM lab_input
WHERE LOWER(`PAYMENT_TYPE`) IN ('telepeagelibert', 'telepeagecardme', 'carteaccreditive', 'cartemagnetique');
