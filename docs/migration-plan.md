# Classement et migration des requetes

Source : `kafka_streams_recovery.log`, commit de reference `3e36d34bee855ea944dbfb665c5beb56f38c7a92`.
Inventaire : **37 traitements persistants** (26 CSAS/CTAS + 11 INSERT distincts), 57 declarations (50 streams, 7 tables).
Les declarations de sources ne sont pas comptees comme traitements. Les repetitions du journal ne creent pas de travail supplementaire.
Les 31 declarations sans AS SELECT restent des contrats de source a reprendre avec les lots consommateurs.

La difficulte concerne la semantique et le contrat Kafka, pas seulement la longueur du SQL.
Les lots regroupent des transformations homogenes ; plusieurs lots sont de difficulte faible.
Le lot 1 est choisi parce qu'il est autonome, repetitif et sans etat, contrairement aux projections dependantes du Socle ET.

| Lot | Difficulte | Nombre | Famille | Statut |
| --- | --- | ---: | --- | --- |
| 1 | Faible | 5 | Lexhistoric ASF, ESC, ARS, ACR, COF | SQL migre ; validation differentielle fournie |
| 2 | Faible | 9 | Consolidation entrees/sorties conformes | SQL migre ; test differentiel fourni |
| 3 | Faible | 2 | Extension classification et projection compteur | Migre ; contrat amont lot 6 conserve |
| 4 | Moyenne | 8 | Bols/coffres (6), synoptiques FAV (2) | Migre ; test differentiel fourni |
| 5 | Elevee | 4 | Histobadge entree/sortie CARDME/CIP | A migrer |
| 6 | Elevee | 4 | Deux referentiels et deux jointures Socle ET | A migrer |
| 7 | Elevee | 1 | Indicateurs horaires | A migrer ; depend du lot 6 |
| 8 | Tres elevee | 4 | Deux sessions classification et deux alarmes | A migrer ; depend du lot 3 |

## Inventaire exhaustif

Les numeros renvoient aux lignes du journal original. Le fichier JSON voisin permet un suivi automatise.

| Lot | Traitement | Ligne | Risque principal |
| --- | --- | ---: | --- |
| 1 | `ACR_PEAGE_LEXVA_PASSAGESORTIEVA_LEXHISTORIC_STREAM` | 1936 | Filtre/projection identique sur cinq sources, sans etat |
| 1 | `ARS_PEAGE_LEXVA_PASSAGESORTIEVA_LEXHISTORIC_STREAM` | 413 | Filtre/projection identique sur cinq sources, sans etat |
| 1 | `ASF_PEAGE_LEXVA_PASSAGESORTIEVA_LEXHISTORIC_STREAM` | 1238 | Filtre/projection identique sur cinq sources, sans etat |
| 1 | `COF_PEAGE_LEXVA_PASSAGESORTIEVA_LEXHISTORIC_STREAM` | 471 | Filtre/projection identique sur cinq sources, sans etat |
| 1 | `ESC_PEAGE_LEXVA_PASSAGESORTIEVA_LEXHISTORIC_STREAM` | 115 | Filtre/projection identique sur cinq sources, sans etat |
| 2 | `IQ_PASSAGEENTREE_CONFORME_VA_FROM_ACR` | 1218 | Consolidation SELECT * ; schemas imbriques, cles et multiplicite |
| 2 | `IQ_PASSAGEENTREE_CONFORME_VA_FROM_COF` | 1385 | Consolidation SELECT * ; schemas imbriques, cles et multiplicite |
| 2 | `IQ_PASSAGEENTREE_CONFORME_VA_FROM_ESC` | 1768 | Consolidation SELECT * ; schemas imbriques, cles et multiplicite |
| 2 | `IQ_PASSAGESORTIE_CONFORME_VA_FROM_ACR` | 585 | Consolidation SELECT * ; schemas imbriques, cles et multiplicite |
| 2 | `IQ_PASSAGESORTIE_CONFORME_VA_FROM_ARS` | 583 | Consolidation SELECT * ; schemas imbriques, cles et multiplicite |
| 2 | `IQ_PASSAGESORTIE_CONFORME_VA_FROM_COF` | 586 | Consolidation SELECT * ; schemas imbriques, cles et multiplicite |
| 2 | `IQ_PASSAGESORTIE_CONFORME_VA_FROM_ESC` | 463 | Consolidation SELECT * ; schemas imbriques, cles et multiplicite |
| 2 | `VA_PEAGE_VOIE_PASSAGEENTREE_CONFORME_VA` | 1823 | Consolidation SELECT * ; schemas imbriques, cles et multiplicite |
| 2 | `VA_PEAGE_VOIE_PASSAGESORTIE_CONFORME_VA` | 557 | Consolidation SELECT * ; schemas imbriques, cles et multiplicite |
| 3 | `VA_PEAGE_BACKOFFICE_SOCLEET_COMPTEUR` | 515 | Projection simple ; dependances amont pour compteur |
| 3 | `VA_PEAGE_CLASSIFICATION_FLOWS_EXTEND` | 372 | Projection simple ; dependances amont pour compteur |
| 4 | `ASF_PEAGE_SUIVICONSO_COMPTAGE_BOL` | 614 | EXPLODE, tableaux de structures, nulls et variantes reseau |
| 4 | `ASF_PEAGE_SUIVICONSO_COMPTAGE_BOL_EXPLODED` | 1016 | EXPLODE, tableaux de structures, nulls et variantes reseau |
| 4 | `ASF_PEAGE_SUIVICONSO_COMPTAGE_COFFRE` | 326 | EXPLODE, tableaux de structures, nulls et variantes reseau |
| 4 | `ASF_PEAGE_TELEOP_SYNOPTIQUE_FAV_STREAM` | 2135 | EXPLODE, tableaux de structures, nulls et variantes reseau |
| 4 | `ESC_PEAGE_SUIVICONSO_COMPTAGE_BOL` | 193 | EXPLODE, tableaux de structures, nulls et variantes reseau |
| 4 | `ESC_PEAGE_SUIVICONSO_COMPTAGE_BOL_EXPLODED` | 713 | EXPLODE, tableaux de structures, nulls et variantes reseau |
| 4 | `ESC_PEAGE_SUIVICONSO_COMPTAGE_COFFRE` | 148 | EXPLODE, tableaux de structures, nulls et variantes reseau |
| 4 | `ESC_PEAGE_TELEOP_SYNOPTIQUE_FAV_STREAM` | 2234 | EXPLODE, tableaux de structures, nulls et variantes reseau |
| 5 | `IQ_HISTOBADGEVA_BE_CARDME` | 1508 | EXPLODE correles, FILTER/TRANSFORM et historiques imbriques |
| 5 | `IQ_HISTOBADGEVA_BE_CIP` | 1509 | EXPLODE correles, FILTER/TRANSFORM et historiques imbriques |
| 5 | `IQ_HISTOBADGEVA_VS_CARDME` | 576 | EXPLODE correles, FILTER/TRANSFORM et historiques imbriques |
| 5 | `IQ_HISTOBADGEVA_VS_CIP` | 577 | EXPLODE correles, FILTER/TRANSFORM et historiques imbriques |
| 6 | `TABLE_VA_PEAGE_BACKOFFICE_SOCLEET_REF_COMPTEURS` | 2811 | LATEST_BY_OFFSET et jointures stream-table sensibles a l ordre |
| 6 | `TABLE_VA_PEAGE_BACKOFFICE_SOCLEET_REF_INDICATEURS` | 2521 | LATEST_BY_OFFSET et jointures stream-table sensibles a l ordre |
| 6 | `VA_PEAGE_BACKOFFICE_SOCLEET_FILTER_REF_COMPTEUR` | 752 | LATEST_BY_OFFSET et jointures stream-table sensibles a l ordre |
| 6 | `VA_PEAGE_BACKOFFICE_SOCLEET_FILTER_REF_INDICATEUR` | 1405 | LATEST_BY_OFFSET et jointures stream-table sensibles a l ordre |
| 7 | `TABLE_VA_PEAGE_BACKOFFICE_SOCLEET_INDICATEUR` | 2899 | Sommes horaires, grace, mises a jour et cles fenetrees |
| 8 | `VA_PEAGE_CLASSIFICATIONINFOQUALITE` | 2572 | Sessions, HLL, mises a jour intermediaires et jointures temporelles |
| 8 | `VA_PEAGE_CLASSIFICATIONINFOQUALITECOUNT` | 2958 | Sessions, HLL, mises a jour intermediaires et jointures temporelles |
| 8 | `VA_PEAGE_CLASSIFICATION_ALARMEGLOBALE` | 2378 | Sessions, HLL, mises a jour intermediaires et jointures temporelles |
| 8 | `VA_PEAGE_CLASSIFICATION_ALARMEUNITAIRE` | 2664 | Sessions, HLL, mises a jour intermediaires et jointures temporelles |

## Lot 1 : contrat de migration

Entrees : `{asf,esc,ars,acr,cof}.peage.voie.passagesortieva` (six partitions dans le laboratoire).
Sortie ksqlDB : `va.peage.voie.passagesortieva.lexhistoric` (deux partitions, retention 24 h).
Sortie Flink de comparaison : meme nom avec suffixe `.flink` ; ne pas activer les deux producteurs sur le topic historique.
Les cinq branches sont reunies avec UNION ALL : aucun DISTINCT, aucune deduplication.

| Regle | Traduction |
| --- | --- |
| IDPASSAGE | IDTRAN, chaine conservee |
| PASSAGESORTIE.DHM | DATETRANSACTION, aucune conversion de date |
| RETENUS[1] ksqlDB | Premier element JSON `[0]`, pas une explosion de tous les paiements |
| TYPE sans tenir compte de la casse | LOWER ; espaces non supprimes |
| telepeagelibert / telepeagecardme / carteaccreditive / cartemagnetique | 256 / 512 / 1024 / 2048 |
| Autres types, paiement absent ou vide | Pas de sortie |
| Premier PAN | CONTENUISO, null conserve |
| SUBSTRING(GARE, 1, 5) | SCA ; gare courte ou nulle conservee selon SQL |
| Noms JSON | UDF NormalizeJsonKeys, normalisation recursive des noms uniquement |
| Cle Kafka | Null en sortie : les DDL originaux ne declarent aucune colonne KEY, meme si les entrees ont des cles |
| Timestamp Kafka | Metadonnee timestamp en lecture et ecriture, independante de DHM |

La reference ksqlDB est extraite du journal avec les schemas imbriques complets ; elle n'est pas une reimplementation de l'oracle.
Le runner change uniquement les noms d'objets/topics, la replication locale a 1, et retire CLEANUP_POLICY :
DESCRIBE affiche cette propriete inferee, mais ksqlDB 7.9.2 refuse qu'on la fournisse explicitement.
La retention et le nombre de partitions de sortie sont fixes explicitement a la creation des topics de test.

Le SQL Flink lit le JSON brut pour eviter une lecture sensible a la casse. L'UDF Java reprend le premier nom correspondant
sans tenir compte de la casse ; les valeurs et la position des tableaux sont conservees. JSON_VALUE extrait les cinq champs utiles.
Ce choix n'est pas un changement de schema source ; les champs inutilises ne sont pas transportes vers la sortie.

## Execution

```powershell
docker compose up -d --build --wait
python scripts/test_lexhistoric.py
```

Le build compile l'UDF et un outil de transport Kafka dans une etape JDK ; le runtime reste Java 17.
Le test emploie cinq topics d'entree et deux sorties uniques par execution ; il charge les cinq requetes ksqlDB et un job Flink.
Les fixtures sont des donnees synthetiques, sans identifiants de paiement reels.
Les requetes de test sont arretees en fin d'execution ; topics et declarations restent inspectables.
Rapport : `artifacts/lex<run-id>/comparison.json`, SQL effectif et log Flink dans le meme dossier.

La comparaison porte sur les objets JSON, leur multiplicite, les octets de cle encodes en base64 et les timestamps Kafka.
Elle ne compare pas l'ordre global entre partitions, les espaces/ordre des proprietes JSON, les offsets de sortie ou la repartition
physique des messages sans cle. Ces elements ne constituent pas une garantie identique entre moteurs.

Le lot n'est pas une certification de production : les garanties de reprise/exactly-once, headers Kafka, JSON malforme,
types incompatibles avec le schema et performances restent hors du domaine valide. JSON malforme fait echouer l'UDF,
alors que ksqlDB peut journaliser puis ignorer le message : aligner la politique d'erreur avant mise en production.
La source de production doit respecter les types STRING/STRUCT/ARRAY declares dans le journal.
Le demarrage earliest-offset est un choix de replay de laboratoire ; definir les offsets de bascule avant deploiement.
Aucun topic de production ni infrastructure externe n'est modifie par ce lot.

## Resultat de validation du lot 1

Execution du 26 octobre 2026 sur Kafka/ksqlDB 7.9.2, Flink 1.20.3, connecteur 3.4.0-1.20 :
`python scripts/test_lexhistoric.py` -> **PASS**, execution `lex23d2edb1`.
18 cas par reseau, soit **90 messages d'entree et 40 sorties attendues sur chaque moteur**.
Les objets JSON, leur multiplicite, les cles nulles et les timestamps Kafka correspondent a l'oracle.
Les cas couvrent quatre modes, trois variantes de casse des noms JSON, Unicode, premier paiement uniquement,
gare courte/vide, valeurs nulles, champs absents, tableaux vides/nulls, premier element null, mode exclu,
espaces non supprimes, tombstone et valeurs de sortie repetees.
Le rapport detaille reste local dans `artifacts/lex23d2edb1/comparison.json` (dossier ignore par Git).
Cette preuve porte sur le domaine decrit ci-dessus ; les exclusions ne sont pas levees par un test vert.

## Utilisation manuelle et future bascule

Pour une execution manuelle du SQL Flink dans le laboratoire, creer les entrees et la sortie avant la soumission :

```powershell
foreach ($net in @('asf', 'esc', 'ars', 'acr', 'cof')) {
  docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic "$net.peage.voie.passagesortieva" --partitions 6 --replication-factor 1
}
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic va.peage.voie.passagesortieva.lexhistoric.flink --partitions 2 --replication-factor 1 --config retention.ms=86400000 --config cleanup.policy=delete
docker compose run --rm -T sql-client embedded -f /opt/flink/sql/02-lexhistoric.sql
```

Avant une bascule de production : relever les offsets par partition, valider les proprietes effectives des topics,
executer Flink sur une sortie miroir et verifier les consommateurs. Arreter les cinq producteurs ksqlDB a une
frontiere documentee, positionner Flink aux offsets suivants et activer sa sortie historique avec un seul moteur
producteur. Garder le plan de retour aux offsets appropries ; ne pas relancer depuis earliest sur le topic historique.
La replication a 1 et la configuration at-least-once du laboratoire ne sont pas des reglages de production approuves.

## Lot 2

Les neuf branches de consolidation sont migrees. Voir [le contrat du lot 2](lot-2-conformite.md) pour les schemas, les regles de serialisation et le test reproductible.

## Lots 3 et 4

Dix traitements supplementaires migres, soit 24 sur 37. Voir [le contrat des lots 3/4](lots-3-4.md). La jointure amont du compteur reste a migrer au lot 6.
