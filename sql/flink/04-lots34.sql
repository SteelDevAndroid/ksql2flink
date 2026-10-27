-- Lots 3/4. Inputs are existing Kafka contracts; outputs are isolated with .flink.
CREATE TEMPORARY SYSTEM FUNCTION KSQL_PROJECT AS 'lab.ProjectKsqlJson';
CREATE TEMPORARY SYSTEM FUNCTION JSON_ITEMS AS 'lab.JsonElements';
CREATE TEMPORARY SYSTEM FUNCTION JSON_PARTS AS 'lab.JsonObjectParts';
CREATE TEMPORARY SYSTEM FUNCTION INTEGER_KEY AS 'lab.IntegerKafkaKey';
CREATE TEMPORARY SYSTEM FUNCTION TRUNCATE_INTEGER AS 'lab.TruncateInteger';
CREATE TEMPORARY SYSTEM FUNCTION JSON_CONTAINS_STRUCT AS 'lab.JsonContains';

CREATE TEMPORARY SYSTEM FUNCTION JSON_STRING AS 'lab.JsonString';
CREATE TABLE flows_input (payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='va.peage.classification.flows',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'scan.startup.mode'='earliest-offset',
  'properties.group.id'='lot34-flows_input');

CREATE VIEW flows AS SELECT kafka_timestamp, KSQL_PROJECT(payload, 'lot34-flows') AS j FROM flows_input;
CREATE TABLE asf_coffre_input (payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='asf.peage.voie.comptage.coffre',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'scan.startup.mode'='earliest-offset',
  'properties.group.id'='lot34-asf_coffre_input');

CREATE VIEW asf_coffre AS SELECT kafka_timestamp, KSQL_PROJECT(payload, 'lot34-asf-coffre') AS j FROM asf_coffre_input;
CREATE TABLE asf_bol_input (payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='asf.peage.voie.comptage.bol',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'scan.startup.mode'='earliest-offset',
  'properties.group.id'='lot34-asf_bol_input');

CREATE VIEW asf_bol AS SELECT kafka_timestamp, KSQL_PROJECT(payload, 'lot34-asf-bol') AS j FROM asf_bol_input;
CREATE TABLE asf_synoptique_input (kafka_key BYTES, payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='asf.peage.voie.synoptique',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'key.format'='raw',
  'key.fields'='kafka_key',
  'value.fields-include'='EXCEPT_KEY',
  'scan.startup.mode'='earliest-offset',
  'properties.group.id'='lot34-asf_synoptique_input');

CREATE VIEW asf_synoptique AS SELECT kafka_key, kafka_timestamp, KSQL_PROJECT(payload, 'lot34-asf-synoptique') AS j FROM asf_synoptique_input;
CREATE TABLE esc_coffre_input (payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='esc.peage.voie.comptage.coffre',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'scan.startup.mode'='earliest-offset',
  'properties.group.id'='lot34-esc_coffre_input');

CREATE VIEW esc_coffre AS SELECT kafka_timestamp, KSQL_PROJECT(payload, 'lot34-esc-coffre') AS j FROM esc_coffre_input;
CREATE TABLE esc_bol_input (payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='esc.peage.voie.comptage.bol',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'scan.startup.mode'='earliest-offset',
  'properties.group.id'='lot34-esc_bol_input');

CREATE VIEW esc_bol AS SELECT kafka_timestamp, KSQL_PROJECT(payload, 'lot34-esc-bol') AS j FROM esc_bol_input;
CREATE TABLE esc_synoptique_input (kafka_key BYTES, payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='esc.peage.voie.synoptique',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'key.format'='raw',
  'key.fields'='kafka_key',
  'value.fields-include'='EXCEPT_KEY',
  'scan.startup.mode'='earliest-offset',
  'properties.group.id'='lot34-esc_synoptique_input');

CREATE VIEW esc_synoptique AS SELECT kafka_key, kafka_timestamp, KSQL_PROJECT(payload, 'lot34-esc-synoptique') AS j FROM esc_synoptique_input;
CREATE TABLE compteur_input_input (kafka_key BYTES, payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='VA_PEAGE_BACKOFFICE_SOCLEET_FILTER_REF_COMPTEUR',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'key.format'='raw',
  'key.fields'='kafka_key',
  'value.fields-include'='EXCEPT_KEY',
  'scan.startup.mode'='earliest-offset',
  'properties.group.id'='lot34-compteur_input_input');

CREATE VIEW compteur_input AS SELECT kafka_key, kafka_timestamp, KSQL_PROJECT(payload, 'lot34-compteur-input') AS j FROM compteur_input_input;
CREATE TABLE flows_extend_output (payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='VA_PEAGE_CLASSIFICATION_FLOWS_EXTEND.flink',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'sink.delivery-guarantee'='at-least-once');

CREATE TABLE compteur_output (kafka_key BYTES, payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='va.peage.backoffice.socleET.compteur.flink',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'key.format'='raw',
  'key.fields'='kafka_key',
  'value.fields-include'='EXCEPT_KEY',
  'sink.delivery-guarantee'='at-least-once');

CREATE TABLE asf_coffre_output (payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='asf.peage.suiviconso.comptage.coffre.flink',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'sink.delivery-guarantee'='at-least-once');

CREATE TABLE esc_coffre_output (payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='esc.peage.suiviconso.comptage.coffre.flink',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'sink.delivery-guarantee'='at-least-once');

CREATE TABLE asf_exploded_output (payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='asf.peage.suiviconso.comptage.bol.exploded.flink',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'sink.delivery-guarantee'='at-least-once');

CREATE TABLE esc_exploded_output (payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='esc.peage.suiviconso.comptage.bol.exploded.flink',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'sink.delivery-guarantee'='at-least-once');

CREATE TABLE asf_bol_output (payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='asf.peage.suiviconso.comptage.bol.flink',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'sink.delivery-guarantee'='at-least-once');

CREATE TABLE esc_bol_output (payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='esc.peage.suiviconso.comptage.bol.flink',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'sink.delivery-guarantee'='at-least-once');

CREATE TABLE asf_fav_output (kafka_key BYTES, payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='asf.peage.teleop.synoptique.fav.flink',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'key.format'='raw',
  'key.fields'='kafka_key',
  'value.fields-include'='EXCEPT_KEY',
  'sink.delivery-guarantee'='at-least-once');

CREATE TABLE esc_fav_output (kafka_key BYTES, payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='esc.peage.teleop.synoptique.fav.flink',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'key.format'='raw',
  'key.fields'='kafka_key',
  'value.fields-include'='EXCEPT_KEY',
  'sink.delivery-guarantee'='at-least-once');

CREATE TABLE asf_exploded_input (payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='asf.peage.suiviconso.comptage.bol.exploded.flink',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'scan.startup.mode'='earliest-offset',
  'properties.group.id'='lot34-asf_exploded_input');

CREATE VIEW asf_exploded AS SELECT payload AS j, kafka_timestamp FROM asf_exploded_input;
CREATE TABLE esc_exploded_input (payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')
WITH ('connector'='kafka',
  'topic'='esc.peage.suiviconso.comptage.bol.exploded.flink',
  'properties.bootstrap.servers'='kafka:9092',
  'value.format'='raw',
  'scan.startup.mode'='earliest-offset',
  'properties.group.id'='lot34-esc_exploded_input');

CREATE VIEW esc_exploded AS SELECT payload AS j, kafka_timestamp FROM esc_exploded_input;
EXECUTE STATEMENT SET
BEGIN
INSERT INTO flows_extend_output SELECT CASE WHEN j IS NULL THEN CAST(NULL AS STRING) ELSE JSON_PARTS(ARRAY['FLOW','BUSINESSFILTER','BUSINESSRULE','BUSINESSCYCLOPE','RESPONSE','TECHNICALFILTER','STATIONLANE'], ARRAY[JSON_QUERY(j, '$.FLOW'),JSON_QUERY(j, '$.BUSINESSFILTER'),JSON_QUERY(j, '$.BUSINESSRULE'),JSON_QUERY(j, '$.BUSINESSCYCLOPE'),JSON_QUERY(j, '$.RESPONSE'),JSON_QUERY(j, '$.TECHNICALFILTER'),JSON_STRING(CONCAT(COALESCE(JSON_VALUE(j, '$.FLOW.STATION'),''),COALESCE(JSON_VALUE(j, '$.FLOW.LANE'),'')))]) END, kafka_timestamp FROM flows;

INSERT INTO compteur_output SELECT kafka_key, CASE WHEN j IS NULL THEN CAST(NULL AS STRING) ELSE JSON_OBJECT('R_COMPTEURID' VALUE INTEGER_KEY(kafka_key),
  'R_VOIEID' VALUE JSON_VALUE(j, '$.R_VOIEID' RETURNING INTEGER),
  'R_GAREID' VALUE JSON_VALUE(j, '$.R_GAREID' RETURNING INTEGER),
  'R_SOCIETEID' VALUE JSON_VALUE(j, '$.R_SOCIETEID' RETURNING INTEGER),
  'R_PARTENAIREID' VALUE JSON_VALUE(j, '$.R_PARTENAIREID' RETURNING INTEGER),
  'DATE_DEBUT' VALUE JSON_VALUE(j, '$.DATE_OPERATION' RETURNING DOUBLE),
  'DATE_FIN' VALUE JSON_VALUE(j, '$.R_FREQUENCEID' RETURNING INTEGER),
  'DATE_COMPTABLE' VALUE JSON_VALUE(j, '$.DATE_COMPTABLE' RETURNING DOUBLE),
  'VALEUR' VALUE JSON_VALUE(j, '$.VALEUR' RETURNING DOUBLE)) END, kafka_timestamp FROM compteur_input;

INSERT INTO asf_coffre_output SELECT JSON_OBJECT('DateMessage' VALUE JSON_VALUE(j, '$.message.DHM'),
  'IdGare' VALUE JSON_VALUE(j, '$.comptageCoffre.localisation.gare'),
  'Voie' VALUE JSON_VALUE(j, '$.comptageCoffre.localisation.equipement.numero'),
  'DateComptage' VALUE JSON_VALUE(j, '$.comptageCoffre.dhm'),
  'Position' VALUE CASE JSON_VALUE(j, '$.comptageCoffre.position' RETURNING INTEGER) WHEN 0 THEN 15 WHEN 1 THEN CASE JSON_VALUE(j, '$.comptageCoffre.contenu.type') WHEN 'piece' THEN 7 WHEN 'billet' THEN 16 ELSE 1 END WHEN 2 THEN 8 WHEN 5 THEN 14 ELSE JSON_VALUE(j, '$.comptageCoffre.position' RETURNING INTEGER) END,
  'Type' VALUE JSON_VALUE(j, '$.comptageCoffre.type'),
  'TypeEspece' VALUE JSON_VALUE(j, '$.comptageCoffre.contenu.type'),
  'ValeurCapacite' VALUE JSON_VALUE(j, '$.comptageCoffre.contenu.capacite.valeur' RETURNING INTEGER),
  'UniteCapacite' VALUE JSON_VALUE(j, '$.comptageCoffre.contenu.capacite.unite'),
  'ValeurCourant' VALUE JSON_VALUE(j, '$.comptageCoffre.contenu.courant.valeur' RETURNING INTEGER),
  'UniteCourant' VALUE JSON_VALUE(j, '$.comptageCoffre.contenu.courant.unite')), kafka_timestamp FROM asf_coffre WHERE JSON_VALUE(j, '$.comptageCoffre.type') IN ('sac','coffre') AND JSON_VALUE(j, '$.comptageCoffre.localisation.gare') LIKE '25004%';

INSERT INTO asf_exploded_output SELECT JSON_PARTS(ARRAY['message','comptageBols','Etat'], ARRAY[JSON_QUERY(j,'$.message'),JSON_QUERY(j,'$.comptageBols'),item]), kafka_timestamp FROM asf_bol CROSS JOIN UNNEST(JSON_ITEMS(JSON_QUERY(j,'$.etats'))) AS e(item);

INSERT INTO asf_bol_output SELECT JSON_OBJECT('DateMessage' VALUE JSON_VALUE(j, '$.message.DHM'),
  'IdGare' VALUE JSON_VALUE(j, '$.comptageBols.localisation.gare'),
  'Voie' VALUE JSON_VALUE(j, '$.comptageBols.localisation.equipement.numero'),
  'DateComptage' VALUE JSON_VALUE(j, '$.comptageBols.dhm'),
  'Position' VALUE CASE JSON_VALUE(j, '$.Etat.contenu.valeur' RETURNING DOUBLE) WHEN 0.1 THEN 9 WHEN 0.2 THEN 10 WHEN 0.5 THEN 11 WHEN 1 THEN 12 WHEN 2 THEN 13 ELSE TRUNCATE_INTEGER(JSON_VALUE(j, '$.Etat.contenu.valeur' RETURNING DOUBLE)) END,
  'Numero' VALUE SPLIT_INDEX(JSON_VALUE(j, '$.Etat.sujet'), '.', 1),
  'Type' VALUE JSON_VALUE(j, '$.Etat.contenu.type'),
  'Devise' VALUE JSON_VALUE(j, '$.Etat.contenu.devise'),
  'Valeur' VALUE JSON_VALUE(j, '$.Etat.contenu.courant' RETURNING INTEGER),
  'Capacite' VALUE JSON_VALUE(j, '$.Etat.contenu.maximum' RETURNING INTEGER)), kafka_timestamp FROM asf_exploded WHERE JSON_VALUE(j, '$.Etat.contenu.type')='piece' AND JSON_VALUE(j, '$.comptageBols.localisation.gare') LIKE '25004%';

INSERT INTO asf_fav_output SELECT kafka_key, JSON_OBJECT('DHM' VALUE JSON_VALUE(j, '$.SYNOPTIQUE.DHM'),
  'GARE' VALUE JSON_VALUE(j, '$.SYNOPTIQUE.LOCALISATION.GARE'),
  'VOIE' VALUE JSON_VALUE(j, '$.SYNOPTIQUE.LOCALISATION.EQUIPEMENT.NUMERO'),
  'GAREGROUP' VALUE 1,
  'ENTREESORTIE' VALUE CASE WHEN JSON_VALUE(j, '$.SYNOPTIQUE.LOCALISATION.EQUIPEMENT.TYPE') IN ('BE','BESA','BET') THEN 'E' ELSE 'S' END,
  'COMLSKO' VALUE CASE WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'comLS','ETAT' VALUE 'comKO')) THEN 1 WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'comLS','ETAT' VALUE 'comOKNTR')) THEN 0 WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'comLS','ETAT' VALUE 'comOKTR')) THEN 0 ELSE CAST(NULL AS INT) END,
  'CONTRESENS' VALUE CASE WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'contreSens','ETAT' VALUE 'actif')) THEN 1 WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'contreSens','ETAT' VALUE 'nonActif')) THEN 0 ELSE CAST(NULL AS INT) END,
  'FAVFLECHEVERTE' VALUE CASE WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.flecheVerte','ETAT' VALUE 'allume')) THEN 1 WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.flecheVerte','ETAT' VALUE 'eteint')) THEN 0 ELSE CAST(NULL AS INT) END,
  'FAVTIS' VALUE CASE WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.TIS','ETAT' VALUE 'allume')) THEN 1 WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.TIS','ETAT' VALUE 'eteint')) THEN 0 ELSE CAST(NULL AS INT) END,
  'FAVCB' VALUE CASE WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.carte','ETAT' VALUE 'allume')) THEN 1 WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.carte','ETAT' VALUE 'eteint')) THEN 0 ELSE CAST(NULL AS INT) END,
  'PAIEMENTESPECE' VALUE CASE WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'paiement.espece','ETAT' VALUE 'accepte')) THEN 1 WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'paiement.espece','ETAT' VALUE 'nonAccepte')) THEN 0 ELSE CAST(NULL AS INT) END), kafka_timestamp FROM asf_synoptique WHERE NOT COALESCE(JSON_VALUE(j, '$.SYNOPTIQUE.LOCALISATION.EQUIPEMENT.TYPE') IN ('BECM','VSP'), FALSE) AND (JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'comLS','ETAT' VALUE 'comKO')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'comLS','ETAT' VALUE 'comOKNTR')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'comLS','ETAT' VALUE 'comOKTR')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'contreSens','ETAT' VALUE 'actif')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'contreSens','ETAT' VALUE 'nonActif')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.flecheVerte','ETAT' VALUE 'allume')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.flecheVerte','ETAT' VALUE 'eteint')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.TIS','ETAT' VALUE 'allume')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.TIS','ETAT' VALUE 'eteint')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.carte','ETAT' VALUE 'allume')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.carte','ETAT' VALUE 'eteint')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'paiement.espece','ETAT' VALUE 'accepte')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'paiement.espece','ETAT' VALUE 'nonAccepte')));

INSERT INTO esc_coffre_output SELECT JSON_OBJECT('DateMessage' VALUE JSON_VALUE(j, '$.message.DHM'),
  'IdGare' VALUE JSON_VALUE(j, '$.comptageCoffre.localisation.gare'),
  'Voie' VALUE JSON_VALUE(j, '$.comptageCoffre.localisation.equipement.numero'),
  'DateComptage' VALUE JSON_VALUE(j, '$.comptageCoffre.dhm'),
  'Position' VALUE CASE JSON_VALUE(j, '$.comptageCoffre.position' RETURNING INTEGER) WHEN 0 THEN 15 WHEN 1 THEN CASE JSON_VALUE(j, '$.comptageCoffre.contenu.type') WHEN 'piece' THEN 7 WHEN 'billet' THEN 16 ELSE 1 END WHEN 2 THEN 8 WHEN 5 THEN 14 ELSE JSON_VALUE(j, '$.comptageCoffre.position' RETURNING INTEGER) END,
  'Type' VALUE JSON_VALUE(j, '$.comptageCoffre.type'),
  'TypeEspece' VALUE JSON_VALUE(j, '$.comptageCoffre.contenu.type'),
  'ValeurCapacite' VALUE JSON_VALUE(j, '$.comptageCoffre.contenu.capacite.valeur' RETURNING INTEGER),
  'UniteCapacite' VALUE JSON_VALUE(j, '$.comptageCoffre.contenu.capacite.unite'),
  'ValeurCourant' VALUE JSON_VALUE(j, '$.comptageCoffre.contenu.courant.valeur' RETURNING INTEGER),
  'UniteCourant' VALUE JSON_VALUE(j, '$.comptageCoffre.contenu.courant.unite')), kafka_timestamp FROM esc_coffre WHERE JSON_VALUE(j, '$.comptageCoffre.type') IN ('sac','coffre') AND JSON_VALUE(j, '$.comptageCoffre.localisation.gare') LIKE '25006%';

INSERT INTO esc_exploded_output SELECT JSON_PARTS(ARRAY['message','comptageBols','Etat'], ARRAY[JSON_QUERY(j,'$.message'),JSON_QUERY(j,'$.comptageBols'),item]), kafka_timestamp FROM esc_bol CROSS JOIN UNNEST(JSON_ITEMS(JSON_QUERY(j,'$.etats'))) AS e(item);

INSERT INTO esc_bol_output SELECT JSON_OBJECT('DateMessage' VALUE JSON_VALUE(j, '$.message.DHM'),
  'IdGare' VALUE JSON_VALUE(j, '$.comptageBols.localisation.gare'),
  'Voie' VALUE JSON_VALUE(j, '$.comptageBols.localisation.equipement.numero'),
  'DateComptage' VALUE JSON_VALUE(j, '$.comptageBols.dhm'),
  'Position' VALUE CASE JSON_VALUE(j, '$.Etat.contenu.valeur' RETURNING DOUBLE) WHEN 0.1 THEN 9 WHEN 0.2 THEN 10 WHEN 0.5 THEN 11 WHEN 1 THEN 12 WHEN 2 THEN 13 ELSE TRUNCATE_INTEGER(JSON_VALUE(j, '$.Etat.contenu.valeur' RETURNING DOUBLE)) END,
  'Numero' VALUE SPLIT_INDEX(JSON_VALUE(j, '$.Etat.sujet'), '.', 1),
  'Type' VALUE JSON_VALUE(j, '$.Etat.contenu.type'),
  'Devise' VALUE JSON_VALUE(j, '$.Etat.contenu.devise'),
  'Valeur' VALUE JSON_VALUE(j, '$.Etat.contenu.courant' RETURNING INTEGER),
  'Capacite' VALUE JSON_VALUE(j, '$.Etat.contenu.maximum' RETURNING INTEGER)), kafka_timestamp FROM esc_exploded WHERE JSON_VALUE(j, '$.Etat.contenu.type')='piece' AND JSON_VALUE(j, '$.comptageBols.localisation.gare') LIKE '25006%' AND JSON_VALUE(j, '$.comptageBols.localisation.equipement.type') <> 'MRB';

INSERT INTO esc_fav_output SELECT kafka_key, JSON_OBJECT('DHM' VALUE JSON_VALUE(j, '$.SYNOPTIQUE.DHM'),
  'GARE' VALUE JSON_VALUE(j, '$.SYNOPTIQUE.LOCALISATION.GARE'),
  'VOIE' VALUE JSON_VALUE(j, '$.SYNOPTIQUE.LOCALISATION.EQUIPEMENT.NUMERO'),
  'GAREGROUP' VALUE CASE WHEN JSON_VALUE(j, '$.SYNOPTIQUE.LOCALISATION.GARE') IN ('25006001', '25006014', '25006010', '25006011', '25006012', '25006013', '25006016', '25006017', '25006019', '25006020', '25006021', '25006024', '25006025', '25006026', '25006027', '25006028', '25006029', '25006031', '25006032', '25006033', '25006034', '25006035', '25006054', '25006056', '25006097') THEN CASE WHEN CAST(JSON_VALUE(j, '$.SYNOPTIQUE.LOCALISATION.EQUIPEMENT.NUMERO') AS INT)<20 THEN 1 ELSE 2 END ELSE 1 END,
  'ENTREESORTIE' VALUE CASE WHEN JSON_VALUE(j, '$.SYNOPTIQUE.LOCALISATION.EQUIPEMENT.TYPE') IN ('BE','BESA','BET','BE ESCOTA') THEN 'E' ELSE 'S' END,
  'COMLSKO' VALUE CASE WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'comLS','ETAT' VALUE 'comKO')) THEN 1 WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'comLS','ETAT' VALUE 'comOKNTR')) THEN 0 WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'comLS','ETAT' VALUE 'comOKTR')) THEN 0 ELSE CAST(NULL AS INT) END,
  'CONTRESENS' VALUE CASE WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'contreSens','ETAT' VALUE 'actif')) THEN 1 WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'contreSens','ETAT' VALUE 'nonActif')) THEN 0 ELSE CAST(NULL AS INT) END,
  'FAVFLECHEVERTE' VALUE CASE WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.flecheVerte','ETAT' VALUE 'allume')) THEN 1 WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.flecheVerte','ETAT' VALUE 'eteint')) THEN 0 ELSE CAST(NULL AS INT) END,
  'FAVTIS' VALUE CASE WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.TIS','ETAT' VALUE 'allume')) THEN 1 WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.TIS','ETAT' VALUE 'eteint')) THEN 0 ELSE CAST(NULL AS INT) END,
  'FAVCB' VALUE CASE WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.carte','ETAT' VALUE 'allume')) THEN 1 WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.carte','ETAT' VALUE 'eteint')) THEN 0 ELSE CAST(NULL AS INT) END,
  'PAIEMENTESPECE' VALUE CASE WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'paiement.espece','ETAT' VALUE 'accepte')) THEN 1 WHEN JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'paiement.espece','ETAT' VALUE 'nonAccepte')) THEN 0 ELSE CAST(NULL AS INT) END), kafka_timestamp FROM esc_synoptique WHERE (JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'comLS','ETAT' VALUE 'comKO')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'comLS','ETAT' VALUE 'comOKNTR')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'comLS','ETAT' VALUE 'comOKTR')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'contreSens','ETAT' VALUE 'actif')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'contreSens','ETAT' VALUE 'nonActif')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.flecheVerte','ETAT' VALUE 'allume')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.flecheVerte','ETAT' VALUE 'eteint')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.TIS','ETAT' VALUE 'allume')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.TIS','ETAT' VALUE 'eteint')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.carte','ETAT' VALUE 'allume')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'feu.affectation.carte','ETAT' VALUE 'eteint')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'paiement.espece','ETAT' VALUE 'accepte')) OR JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE 'paiement.espece','ETAT' VALUE 'nonAccepte')));
END;
