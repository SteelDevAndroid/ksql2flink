# Tableau de correspondance ksqlDB → Flink

Ce tableau couvre les **37 traitements persistants** de [l’inventaire](query-inventory.json),
classés dans les huit lots. Chaque nom ksqlDB ouvre sa requête de référence extraite
du journal ; chaque équivalent Flink ouvre le code SQL ou Java réellement livré.
Les colonnes de transformation sont des résumés ; les liens donnent le code exécutable complet.

Les déclarations de sources seules, les doublons du journal et le smoke test ne
sont pas comptés comme des traitements à convertir.

## Comment lire les correspondances

- **Lots 1 à 5 : SQL Flink**, avec les fonctions Java de compatibilité utilisées dans les fichiers SQL.
- **Lots 6 à 8 : DataStream Flink**. Leur équivalent est un opérateur Java configuré par JSON ;
  aucun SQL Flink fictif n’est présenté comme implémentation de ces traitements.
- Plusieurs requêtes ksqlDB peuvent alimenter le même INSERT Flink avec UNION ALL.
  Une ligne par requête est conservée afin de rendre la couverture vérifiable.
- Les sorties indiquées sont les **topics isolés de comparaison**, suffixés `.flink`.
  Elles ne constituent pas un changement des topics de production.

## Lot 1 : Lexhistoric — 5 requêtes

| Requête ksqlDB de référence | Équivalent Flink livré | Transformation conservée | Topic de sortie Flink |
| --- | --- | --- | --- |
| [ACR_PEAGE_LEXVA_PASSAGESORTIEVA_LEXHISTORIC_STREAM](../sql/ksqldb/02-lexhistoric.sql#L41) | [acr_lexhistoric](../sql/flink/02-lexhistoric.sql#L81) → [lexhistoric_output](../sql/flink/02-lexhistoric.sql#L108) | Premier paiement/PAN, filtre des modes et CASE 256/512/1024/2048. | `va.peage.voie.passagesortieva.lexhistoric.flink` |
| [ARS_PEAGE_LEXVA_PASSAGESORTIEVA_LEXHISTORIC_STREAM](../sql/ksqldb/02-lexhistoric.sql#L29) | [ars_lexhistoric](../sql/flink/02-lexhistoric.sql#L63) → [lexhistoric_output](../sql/flink/02-lexhistoric.sql#L108) | Premier paiement/PAN, filtre des modes et CASE 256/512/1024/2048. | `va.peage.voie.passagesortieva.lexhistoric.flink` |
| [ASF_PEAGE_LEXVA_PASSAGESORTIEVA_LEXHISTORIC_STREAM](../sql/ksqldb/02-lexhistoric.sql#L5) | [asf_lexhistoric](../sql/flink/02-lexhistoric.sql#L27) → [lexhistoric_output](../sql/flink/02-lexhistoric.sql#L108) | Premier paiement/PAN, filtre des modes et CASE 256/512/1024/2048. | `va.peage.voie.passagesortieva.lexhistoric.flink` |
| [COF_PEAGE_LEXVA_PASSAGESORTIEVA_LEXHISTORIC_STREAM](../sql/ksqldb/02-lexhistoric.sql#L53) | [cof_lexhistoric](../sql/flink/02-lexhistoric.sql#L99) → [lexhistoric_output](../sql/flink/02-lexhistoric.sql#L108) | Premier paiement/PAN, filtre des modes et CASE 256/512/1024/2048. | `va.peage.voie.passagesortieva.lexhistoric.flink` |
| [ESC_PEAGE_LEXVA_PASSAGESORTIEVA_LEXHISTORIC_STREAM](../sql/ksqldb/02-lexhistoric.sql#L17) | [esc_lexhistoric](../sql/flink/02-lexhistoric.sql#L45) → [lexhistoric_output](../sql/flink/02-lexhistoric.sql#L108) | Premier paiement/PAN, filtre des modes et CASE 256/512/1024/2048. | `va.peage.voie.passagesortieva.lexhistoric.flink` |

## Lot 2 : Passages conformes — 9 requêtes

| Requête ksqlDB de référence | Équivalent Flink livré | Transformation conservée | Topic de sortie Flink |
| --- | --- | --- | --- |
| [IQ_PASSAGEENTREE_CONFORME_VA_FROM_ACR](../sql/ksqldb/03-conformite.sql#L36) | [entree_acr](../sql/flink/03-conformite.sql#L123) → [entree_output](../sql/flink/03-conformite.sql#L115) | Branche UNION ALL ; KSQL_PROJECT(payload, 'entree') ; sans déduplication. | `va.peage.backoffice.conformite.entree.conforme.flink` |
| [IQ_PASSAGEENTREE_CONFORME_VA_FROM_COF](../sql/ksqldb/03-conformite.sql#L38) | [entree_cof](../sql/flink/03-conformite.sql#L126) → [entree_output](../sql/flink/03-conformite.sql#L115) | Branche UNION ALL ; KSQL_PROJECT(payload, 'entree') ; sans déduplication. | `va.peage.backoffice.conformite.entree.conforme.flink` |
| [IQ_PASSAGEENTREE_CONFORME_VA_FROM_ESC](../sql/ksqldb/03-conformite.sql#L40) | [entree_esc](../sql/flink/03-conformite.sql#L120) → [entree_output](../sql/flink/03-conformite.sql#L115) | Branche UNION ALL ; KSQL_PROJECT(payload, 'entree') ; sans déduplication. | `va.peage.backoffice.conformite.entree.conforme.flink` |
| [IQ_PASSAGESORTIE_CONFORME_VA_FROM_ACR](../sql/ksqldb/03-conformite.sql#L32) | [sortie_acr](../sql/flink/03-conformite.sql#L138) → [sortie_output](../sql/flink/03-conformite.sql#L127) | Branche UNION ALL ; KSQL_PROJECT(payload, 'sortie') ; sans déduplication. | `va.peage.backoffice.conformite.sortie.conforme.flink` |
| [IQ_PASSAGESORTIE_CONFORME_VA_FROM_ARS](../sql/ksqldb/03-conformite.sql#L30) | [sortie_ars](../sql/flink/03-conformite.sql#L135) → [sortie_output](../sql/flink/03-conformite.sql#L127) | Branche UNION ALL ; KSQL_PROJECT(payload, 'sortie') ; sans déduplication. | `va.peage.backoffice.conformite.sortie.conforme.flink` |
| [IQ_PASSAGESORTIE_CONFORME_VA_FROM_COF](../sql/ksqldb/03-conformite.sql#L34) | [sortie_cof](../sql/flink/03-conformite.sql#L141) → [sortie_output](../sql/flink/03-conformite.sql#L127) | Branche UNION ALL ; KSQL_PROJECT(payload, 'sortie') ; sans déduplication. | `va.peage.backoffice.conformite.sortie.conforme.flink` |
| [IQ_PASSAGESORTIE_CONFORME_VA_FROM_ESC](../sql/ksqldb/03-conformite.sql#L28) | [sortie_esc](../sql/flink/03-conformite.sql#L132) → [sortie_output](../sql/flink/03-conformite.sql#L127) | Branche UNION ALL ; KSQL_PROJECT(payload, 'sortie') ; sans déduplication. | `va.peage.backoffice.conformite.sortie.conforme.flink` |
| [VA_PEAGE_VOIE_PASSAGEENTREE_CONFORME_VA](../sql/ksqldb/03-conformite.sql#L10) | [entree_asf](../sql/flink/03-conformite.sql#L117) → [entree_output](../sql/flink/03-conformite.sql#L115) | Branche UNION ALL ; KSQL_PROJECT(payload, 'entree') ; sans déduplication. | `va.peage.backoffice.conformite.entree.conforme.flink` |
| [VA_PEAGE_VOIE_PASSAGESORTIE_CONFORME_VA](../sql/ksqldb/03-conformite.sql#L24) | [sortie_asf](../sql/flink/03-conformite.sql#L129) → [sortie_output](../sql/flink/03-conformite.sql#L127) | Branche UNION ALL ; KSQL_PROJECT(payload, 'sortie') ; sans déduplication. | `va.peage.backoffice.conformite.sortie.conforme.flink` |

## Lot 3 : Projections — 2 requêtes

| Requête ksqlDB de référence | Équivalent Flink livré | Transformation conservée | Topic de sortie Flink |
| --- | --- | --- | --- |
| [VA_PEAGE_BACKOFFICE_SOCLEET_COMPTEUR](../sql/ksqldb/04-lots34.sql#L24) | [compteur_output](../sql/flink/04-lots34.sql#L192) | Projection avec INTEGER_KEY(kafka_key) et copie de la clé dans R_COMPTEURID. | `va.peage.backoffice.socleET.compteur.flink` |
| [VA_PEAGE_CLASSIFICATION_FLOWS_EXTEND](../sql/ksqldb/04-lots34.sql#L18) | [flows_extend_output](../sql/flink/04-lots34.sql#L190) | Projection du schéma et CONCAT station/voie ; null traité comme chaîne vide. | `VA_PEAGE_CLASSIFICATION_FLOWS_EXTEND.flink` |

## Lot 4 : Comptages et FAV — 8 requêtes

| Requête ksqlDB de référence | Équivalent Flink livré | Transformation conservée | Topic de sortie Flink |
| --- | --- | --- | --- |
| [ASF_PEAGE_SUIVICONSO_COMPTAGE_BOL](../sql/ksqldb/04-lots34.sql#L84) | [asf_bol_output](../sql/flink/04-lots34.sql#L216) | Lecture du topic exploded, CASE des positions et second segment du sujet. | `asf.peage.suiviconso.comptage.bol.flink` |
| [ASF_PEAGE_SUIVICONSO_COMPTAGE_BOL_EXPLODED](../sql/ksqldb/04-lots34.sql#L70) | [asf_exploded_output](../sql/flink/04-lots34.sql#L214) | CROSS JOIN UNNEST des états ; conservation du topic intermédiaire. | `asf.peage.suiviconso.comptage.bol.exploded.flink` |
| [ASF_PEAGE_SUIVICONSO_COMPTAGE_COFFRE](../sql/ksqldb/04-lots34.sql#L38) | [asf_coffre_output](../sql/flink/04-lots34.sql#L202) | Filtre sac/coffre, gare réseau et CASE sur les positions. | `asf.peage.suiviconso.comptage.coffre.flink` |
| [ASF_PEAGE_TELEOP_SYNOPTIQUE_FAV_STREAM](../sql/ksqldb/04-lots34.sql#L114) | [asf_fav_output](../sql/flink/04-lots34.sql#L227) | JSON_CONTAINS_STRUCT et CASE ordonnés ; règles propres au réseau. | `asf.peage.teleop.synoptique.fav.flink` |
| [ESC_PEAGE_SUIVICONSO_COMPTAGE_BOL](../sql/ksqldb/04-lots34.sql#L99) | [esc_bol_output](../sql/flink/04-lots34.sql#L253) | Lecture du topic exploded, CASE des positions et second segment du sujet. | `esc.peage.suiviconso.comptage.bol.flink` |
| [ESC_PEAGE_SUIVICONSO_COMPTAGE_BOL_EXPLODED](../sql/ksqldb/04-lots34.sql#L77) | [esc_exploded_output](../sql/flink/04-lots34.sql#L251) | CROSS JOIN UNNEST des états ; conservation du topic intermédiaire. | `esc.peage.suiviconso.comptage.bol.exploded.flink` |
| [ESC_PEAGE_SUIVICONSO_COMPTAGE_COFFRE](../sql/ksqldb/04-lots34.sql#L54) | [esc_coffre_output](../sql/flink/04-lots34.sql#L239) | Filtre sac/coffre, gare réseau et CASE sur les positions. | `esc.peage.suiviconso.comptage.coffre.flink` |
| [ESC_PEAGE_TELEOP_SYNOPTIQUE_FAV_STREAM](../sql/ksqldb/04-lots34.sql#L132) | [esc_fav_output](../sql/flink/04-lots34.sql#L264) | JSON_CONTAINS_STRUCT et CASE ordonnés ; règles propres au réseau. | `esc.peage.teleop.synoptique.fav.flink` |

## Lot 5 : Historiques badges — 4 requêtes

| Requête ksqlDB de référence | Équivalent Flink livré | Transformation conservée | Topic de sortie Flink |
| --- | --- | --- | --- |
| [IQ_HISTOBADGEVA_BE_CARDME](../sql/ksqldb/05-badges.sql#L15) | [badge_entree_output / CARDME](../sql/flink/05-badges.sql#L32) | UNNEST corrélé par événement CARDME, filtres gare/PAN et BADGE_HISTORY(event_json, 'CARDME'). | `va.peage.histobadgeva.be.all.flink` |
| [IQ_HISTOBADGEVA_BE_CIP](../sql/ksqldb/05-badges.sql#L17) | [badge_entree_output / CIP](../sql/flink/05-badges.sql#L38) | UNNEST corrélé par événement CIP, filtres gare/PAN et BADGE_HISTORY(event_json, 'CIP'). | `va.peage.histobadgeva.be.all.flink` |
| [IQ_HISTOBADGEVA_VS_CARDME](../sql/ksqldb/05-badges.sql#L11) | [badge_sortie_output / CARDME](../sql/flink/05-badges.sql#L44) | UNNEST corrélé par événement CARDME, filtres gare/PAN et BADGE_HISTORY(event_json, 'CARDME'). | `va.peage.histobadgeva.vs.all.flink` |
| [IQ_HISTOBADGEVA_VS_CIP](../sql/ksqldb/05-badges.sql#L13) | [badge_sortie_output / CIP](../sql/flink/05-badges.sql#L50) | UNNEST corrélé par événement CIP, filtres gare/PAN et BADGE_HISTORY(event_json, 'CIP'). | `va.peage.histobadgeva.vs.all.flink` |

## Lot 6 : Référentiels et enrichissements Socle ET — 4 requêtes

| Requête ksqlDB de référence | Équivalent Flink livré | Transformation conservée | Topic de sortie Flink |
| --- | --- | --- | --- |
| [TABLE_VA_PEAGE_BACKOFFICE_SOCLEET_REF_COMPTEURS](../sql/ksqldb/06-socleet.sql#L7) | [AggregateReference(fields compteur)](../docker/flink/src/lab/SocleEtJob.java#L82) ; UID `compteur-aggregate` | COUNT et dernière référence acceptée ; timestamp maximal ; clé INTEGER Kafka. | `TABLE_VA_PEAGE_BACKOFFICE_SOCLEET_REF_COMPTEURS.flink` |
| [TABLE_VA_PEAGE_BACKOFFICE_SOCLEET_REF_INDICATEURS](../sql/ksqldb/06-socleet.sql#L20) | [AggregateReference(fields indicateur)](../docker/flink/src/lab/SocleEtJob.java#L82) ; UID `indicateur-aggregate` | COUNT et dernière référence acceptée ; timestamp maximal ; clé INTEGER Kafka. | `TABLE_VA_PEAGE_BACKOFFICE_SOCLEET_REF_INDICATEURS.flink` |
| [VA_PEAGE_BACKOFFICE_SOCLEET_FILTER_REF_COMPTEUR](../sql/ksqldb/06-socleet.sql#L30) | [Enrich(true)](../docker/flink/src/lab/SocleEtJob.java#L104) ; UID `compteur-lookup` | Jointure déclenchée par les faits ; filtres opération/état/mode. | `VA_PEAGE_BACKOFFICE_SOCLEET_FILTER_REF_COMPTEUR.flink` |
| [VA_PEAGE_BACKOFFICE_SOCLEET_FILTER_REF_INDICATEUR](../sql/ksqldb/06-socleet.sql#L49) | [Enrich(false)](../docker/flink/src/lab/SocleEtJob.java#L104) ; UID `indicateur-lookup` | Jointure déclenchée par les faits ; référence indicateur existante ; calcul de VALEUR. | `VA_PEAGE_BACKOFFICE_SOCLEET_FILTER_REF_INDICATEUR.flink` |

## Lot 7 : Indicateur horaire — 1 requête

| Requête ksqlDB de référence | Équivalent Flink livré | Transformation conservée | Topic de sortie Flink |
| --- | --- | --- | --- |
| [TABLE_VA_PEAGE_BACKOFFICE_SOCLEET_INDICATEUR](../sql/ksqldb/07-08-windows.sql#L5) | [Hourly](../docker/flink/src/lab/WindowJob.java#L87) ; UID `hourly-aggregate` | SUM incrémental, fenêtre horaire UTC, grâce historique et clé binaire de fenêtre. | `va.peage.backoffice.socleET.indicateur.flink` |

## Lot 8 : Sessions et alarmes — 4 requêtes

| Requête ksqlDB de référence | Équivalent Flink livré | Transformation conservée | Topic de sortie Flink |
| --- | --- | --- | --- |
| [VA_PEAGE_CLASSIFICATIONINFOQUALITE](../sql/ksqldb/07-08-windows.sql#L24) | [Sessions(true)](../docker/flink/src/lab/WindowJob.java#L136) ; UID `quality-sessions` | Sessions 60 min ; ratio > 0,3 et NBPASSAGE >= 20 ; fusions et tombstones. | `va.peage.classificationInfoQualite.flink` |
| [VA_PEAGE_CLASSIFICATIONINFOQUALITECOUNT](../sql/ksqldb/07-08-windows.sql#L47) | [Sessions(false)](../docker/flink/src/lab/WindowJob.java#L136) ; UID `count-sessions` | Sessions 60 min ; COUNT_DISTINCT par HyperLogLog(14), bornes et tombstones. | `va.peage.classificationInfoQualite.Count.flink` |
| [VA_PEAGE_CLASSIFICATION_ALARMEGLOBALE](../sql/ksqldb/07-08-windows.sql#L90) | [AlarmJoin](../docker/flink/src/lab/WindowJob.java#L199) → [AlarmAggregate(true)](../docker/flink/src/lab/WindowJob.java#L240) ; UID `global_alarm-aggregate` | AlarmJoin ±60 s ; LATEST_BY_OFFSET ; seuil >= 30 ; regroupement société. | `va.peage.classificationAlarme.flink` |
| [VA_PEAGE_CLASSIFICATION_ALARMEUNITAIRE](../sql/ksqldb/07-08-windows.sql#L60) | [AlarmJoin](../docker/flink/src/lab/WindowJob.java#L199) → [AlarmAggregate(false)](../docker/flink/src/lab/WindowJob.java#L240) ; UID `unit_alarm-aggregate` | AlarmJoin ±60 s ; LATEST_BY_OFFSET ; seuil < 30 ; regroupement gare/voie. | `va.peage.classificationAlarme.flink` |

## Regroupements et dépendances à conserver

| Correspondance | Conséquence pour l’utilisation |
| --- | --- |
| Les cinq requêtes Lexhistoric → un INSERT vers lexhistoric_output | Les cinq vues réseau alimentent UNION ALL ; aucune déduplication n’est ajoutée. |
| Les neuf requêtes de consolidation → deux INSERT entrée/sortie | Les CSAS ASF et les INSERT des autres réseaux deviennent des branches du même UNION ALL. ARS entrée reste exclu car absent des INSERT du journal. |
| Les quatre requêtes badges → deux sorties partagées | Les INSERT CARDME et CIP restent distincts, avec une sortie commune par sens. L’explosion conserve la corrélation PAN/historique/D_VET. |
| Les référentiels du lot 6 → les deux jointures d’enrichissement | Les topics de tables matérialisées sont conservés ; une mise à jour de référence ne réémet pas les transactions passées. |
| L’enrichissement compteur du lot 6 → la projection compteur du lot 3 | La projection ne remplace pas la jointure amont. |
| L’enrichissement indicateur du lot 6 → l’agrégat horaire du lot 7 | Le job de fenêtres doit lire la sortie du moteur retenu pour la chaîne. |
| FLOWS_EXTEND du lot 3 → qualité → comptage → alarmes du lot 8 | Les alarmes lisent qualité ET comptage. Ces topics physiques restent des frontières de traitement. |
| Les deux alarmes → un même topic classificationAlarme.flink | Les deux regroupements et leurs clés restent distincts malgré leur destination commune. |

## Fichiers d’exécution et validation

| Lots | Point d’entrée Flink | Test de comparaison |
| --- | --- | --- |
| 1 | [02-lexhistoric.sql](../sql/flink/02-lexhistoric.sql) | [test_lexhistoric.py](../scripts/test_lexhistoric.py) |
| 2 | [03-conformite.sql](../sql/flink/03-conformite.sql) | [test_conformite.py](../scripts/test_conformite.py) |
| 3/4 | [04-lots34.sql](../sql/flink/04-lots34.sql) | [test_lots34.py](../scripts/test_lots34.py) |
| 5 | [05-badges.sql](../sql/flink/05-badges.sql) | [test_badges.py](../scripts/test_badges.py) |
| 6 | lab.SocleEtJob + [06-socleet.json](../sql/flink/06-socleet.json) | [test_socleet.py](../scripts/test_socleet.py) |
| 7/8 | lab.WindowJob + [07-08-windows.json](../sql/flink/07-08-windows.json) | [test_windows.py](../scripts/test_windows.py) |

Le mapping établit une traçabilité du code livré ; il ne certifie pas à lui seul
l’iso en production. Les contrats Kafka, les réglages de replay, le câblage entre
les lots et les limites liées aux états et aux flux concurrents sont décrits dans
le [rapport détaillé](rapport-migration-detaille.md) et le [guide de migration](conseils-migration.md).
