# Architecture cible de production

Cible on-prem : Kafka existant, cluster Kubernetes, Flink 1.20 LTS (version du laboratoire).
Ce document décrit la plateforme ; il ne remplace pas les décisions de
[conseils-migration.md](conseils-migration.md).

```
Git (SQL + FlinkDeployment + image) --Argo CD--> Kubernetes
  ns flink-operator : Flink Kubernetes Operator
  ns flink-prod     : un FlinkDeployment par chaîne (JM HA + TM, RocksDB sur SSD local)
        |                         |
        v                         v
  S3 on-prem (checkpoints,   Kafka on-prem (SASL_SSL, ACL par job,
  savepoints, HA)            group.id et transactional.id versionnés)
```

## Choix

| Sujet | Décision | Raison |
| --- | --- | --- |
| Mode | Application mode, un job par chaîne | Isolation des pannes et upgrades indépendants, comme une requête ksqlDB |
| Soumission SQL | `lab.SqlRunner` dans l'image, script en argument | Flink 1.20 ne propose pas `sql-client -f` en application mode |
| Catalogue | DDL dans chaque script, versionnés dans Git | Remplace le command topic ksqlDB sans metastore supplémentaire |
| État | RocksDB incrémental, répertoire de travail sur SSD local | États de jointures et d'agrégats non bornés |
| Stockage durable | S3 compatible on-prem ; NFS/PVC RWX en repli | Checkpoints, savepoints et métadonnées HA |
| HA | `high-availability.type: kubernetes` | Pas de ZooKeeper |
| Upgrades | `upgradeMode: savepoint` et savepoints périodiques | Reprise sans perte d'état |
| Livraison | Sink `exactly-once` et consommateurs `read_committed` | Les sorties du laboratoire sont at-least-once |
| `maxParallelism` | Fixé dès la création (720) | Le modifier invalide l'état |
| Autoscaler | Désactivé pour les lots 7/8 | Partitionnement calculé pour six partitions |

Exemple de déploiement : [deploy/k8s/lot2-conformite.yaml](../deploy/k8s/lot2-conformite.yaml).

## SqlRunner

- Exécute les instructions dans l'ordre, regroupe `EXECUTE STATEMENT SET ... END` en un seul job.
- Un script ne doit produire qu'un job : un `INSERT` ou un `STATEMENT SET` (application mode avec HA).
- Résout `${VAR}` depuis l'environnement (Secrets Kubernetes) ; une variable absente fait échouer le démarrage.
- Les valeurs sont échappées pour être placées dans des littéraux SQL.

## Évolution des requêtes

Une requête Flink SQL modifiée, ou une nouvelle version de Flink, ne garantit pas la reprise
depuis un savepoint : les identifiants d'opérateurs sont générés par le planner.
`COMPILE PLAN` fige le plan pour les montées de version Flink. Pour un changement de logique,
appliquer un déploiement bleu/vert : job `-v2`, nouveau `group.id`, nouveau préfixe
transactionnel, topic de sortie `.v2`, reconstruction de l'état, comparaison puis bascule
des consommateurs.

## Kafka

- Un principal SASL par job, ACL limitées à ses topics, son `group.id` et son préfixe `transactional.id`.
- `transaction.timeout.ms` du sink inférieur à `transaction.max.timeout.ms` du broker et supérieur
  à l'intervalle de checkpoint augmenté du temps de reprise.
- Topics créés par l'IaC ; `auto.create.topics.enable` reste désactivé.

## Supervision

Alertes minimales : lag consommateur (`pendingRecords`), durée et échecs de checkpoint,
`backPressuredTimeMsPerSecond`, `numRestarts`, taille d'état.

## Écart avec une politique « Flink SQL uniquement »

Les lots 6 à 8 sont des jobs DataStream (`lab.SocleEtJob`, `lab.WindowJob`) et les lots SQL
utilisent des UDF Java. La plateforme les déploie de la même façon (seule `entryClass` change).
Une politique strictement SQL imposerait de les réécrire et de requalifier les sessions,
le HyperLogLog et l'ordre des entrelacements.

## Restant à faire avant production

- Paramétrer les scripts `sql/flink` (`${KAFKA_BOOTSTRAP}`, sécurité, `exactly-once`).
- Valider la reprise sur panne et les doublons avec les sinks transactionnels.
- Dimensionner les ressources sur les débits réels.
