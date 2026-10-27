# Argumentaire : migrer les traitements ksqlDB vers Apache Flink

Date : 27 octobre 2026. Public : direction technique, architecture, exploitation.
Pièces justificatives : [rapport détaillé](rapport-migration-detaille.md),
[mapping des 37 traitements](mapping-ksql-flink.md), [audit des requêtes Flink](audit-requetes-flink.md),
[guide de bascule](conseils-migration.md).

## 1. Décision demandée

Remplacer les 37 traitements persistants ksqlDB (Confluent Platform 7.9.2) par des jobs
Apache Flink 1.20, avec une exécution en parallèle sur des topics isolés, puis une bascule lot par lot.
Les contrats Kafka (topics, clés, valeurs JSON, timestamps) restent inchangés pour les consommateurs.

## 2. Pourquoi maintenant : le coût de l'inaction

| Constat | Conséquence |
| --- | --- |
| Confluent Platform 7.9.x sort du **support standard le 19 février 2027** (support Platinum : 19 février 2028) | Dans environ 4 mois, plus de correctifs standard pour le moteur qui porte des flux de péage et de back-office. Rester en place impose soit un contrat Platinum, soit une montée en 8.x, qui demande elle-même une requalification |
| La documentation ksqlDB est rangée par Confluent dans une rubrique *Legacy Documentation* ; l'investissement de l'éditeur en traitement de flux porte sur Flink | Moteur sans trajectoire d'évolution : pas de nouvelles fonctions, communauté qui se réduit, recrutement plus difficile |
| Une montée de version ksqlDB n'est pas gratuite : requêtes persistantes recompilées, topics internes recréés, états reconstruits | Le coût de requalification existe de toute façon. Autant le payer une fois, vers une cible pérenne |
| Le journal `kafka_streams_recovery.log` montre des reprises Kafka Streams | La reprise après incident, opaque dans ksqlDB, est un risque d'exploitation actuel |

## 3. Bénéfices techniques de Flink

### 3.1 Maîtrise de l'état et des reprises

- **Checkpoints et savepoints** : l'état est capturé de façon cohérente avec les offsets Kafka.
  Un job peut être arrêté, mis à jour, redimensionné ou déplacé puis redémarré **sans perte ni recalcul**.
  Dans ksqlDB, l'état vit dans des topics changelog internes et se reconstruit au redémarrage, parfois longuement.
- **Retour arrière contrôlé** : on redémarre depuis un savepoint antérieur. ksqlDB n'offre pas d'équivalent.
- **Mise à l'échelle** : le parallélisme se change au redémarrage depuis un savepoint. Avec ksqlDB,
  le plafond est le nombre de partitions et le rééquilibrage reconstruit l'état.

### 3.2 Sémantique et expressivité

- **Exactly-once de bout en bout** disponible (sinks Kafka transactionnels) quand un flux le
  justifie. La migration reste en at-least-once, à l'identique de ksqlDB.
- **Temps d'événement maîtrisé** : watermarks, gestion explicite des retards et de l'inactivité des sources.
  ksqlDB repose sur un « stream time » implicite par partition, que le laboratoire a dû reproduire à la main pour rester iso.
- **Deux niveaux d'abstraction dans un même moteur** : SQL pour les 28 traitements sans état ou
  simples (lots 1 à 5) ; API DataStream pour les 9 traitements à état fin (lots 6 à 8 : jointures sans
  recalcul rétroactif, sessions avec tombstones, alarmes). Avec ksqlDB, un besoin hors SQL oblige à sortir du moteur.
- **Fonctions Java testables unitairement** (UDF) au lieu des lambdas `TRANSFORM`/`FILTER`/`REDUCE` de ksqlDB, difficiles à tester et à déboguer.

### 3.3 Exploitation

- **Isolation** : un job par famille de traitements, avec ses propres ressources, sa propre
  reprise et son propre cycle de déploiement. Sur un serveur ksqlDB, les requêtes partagent les
  ressources et un incident ou une requête coûteuse affecte les voisines.
- **Observabilité** : back-pressure, lag par source, taille d'état, durée des checkpoints, exposés
  par opérateur via l'interface web et les métriques Prometheus/JMX.
- **Déploiement standard** : opérateur Kubernetes Apache Flink (déploiement déclaratif, savepoints
  automatiques à la mise à jour), ou offres managées de plusieurs fournisseurs.

### 3.4 Pérennité et indépendance

- Projet de la fondation Apache, gouvernance ouverte, large adoption industrielle.
- **Pas de verrou fournisseur** : le même code tourne sur un cluster interne ou chez plusieurs hébergeurs
  (Confluent Cloud, AWS Managed Service for Apache Flink, Ververica, Aiven…). ksqlDB n'existe que chez Confluent.
- Compétences transférables et plus faciles à recruter (Java/SQL standard plutôt qu'un dialecte propriétaire).

## 4. Preuves déjà acquises

| Élément | État |
| --- | --- |
| Couverture | 37/37 traitements implémentés, mapping requête par requête |
| Équivalence | Tests différentiels automatisés : chaque traitement est exécuté en parallèle sur ksqlDB et Flink avec les mêmes entrées. Valeurs, clés, timestamps et multiplicités sont comparés |
| Cas limites couverts | Casse JSON variable, nulls, champs absents ou inconnus, tableaux vides, doublons exacts, tombstones, mises à jour et suppressions de référentiels, fenêtres et sessions avec grâce |
| Reproductibilité | SQL régénéré depuis le journal d'origine ; la CI vérifie l'absence de dérive et rejoue les comparaisons |
| Qualité du code généré | Audit du 27/10/2026 : sources dupliquées supprimées (lot 1 : 147 → 48 lignes ; lot 2 : 142 → 51 lignes), aller-retour Kafka inutile supprimé (lots 3/4), sorties toujours identiques |

## 5. Risques de la migration et parades

| Risque | Parade |
| --- | --- |
| Écart de comportement non couvert par les jeux de test | Exécution en parallèle en préproduction sur des topics `.flink`, avec comparaison continue avant chaque bascule |
| Reprise d'état des lots 6 à 8 (référentiels, fenêtres ouvertes) | Procédure de reconstruction décrite dans le guide ; bascule à une frontière temporelle maîtrisée |
| Doublons à la bascule | Démarrage sur des offsets alignés (`group-offsets`/`timestamp`), jamais un replay depuis le début vers un topic de production |
| Points de robustesse relevés dans les jobs DataStream (partitions codées en dur, état non purgé) | Corrections planifiées avant la préproduction ([audit, section 4](audit-requetes-flink.md#4-jobs-datastream-lots-6-à-8--risques-avant-production)) |
| Complexité d'exploitation d'un cluster Flink | Opérateur Kubernetes ou offre managée ; runbooks fondés sur les checkpoints et savepoints |
| Fonctions de compatibilité JSON (casse) propres à ksqlDB | Limitées en nombre et testées ; supprimables une fois les producteurs normalisés |

Retour arrière : tant que la bascule d'un lot n'est pas confirmée, la requête ksqlDB reste
déployable ; les consommateurs repointent vers le topic historique. Aucun contrat Kafka n'étant modifié, le retour est immédiat.

## 6. Ce que la migration n'apporte pas

Pour être honnête vis-à-vis du décideur :

- Pas de gain fonctionnel immédiat : la cible est **iso**, volontairement.
- Flink demande plus de compétences d'exploitation qu'un serveur ksqlDB unique.
- 9 traitements sur 37 sont en Java DataStream, pas en SQL : c'est plus de code à maintenir, mais du code testable et versionné.

Ces coûts sont inférieurs au risque d'un moteur hors support standard en février 2027, et ils sont déjà largement absorbés par le travail livré.

## 7. Plan de passage recommandé

1. Corriger les points de robustesse DataStream (audit, section 4) et figer les plans SQL (`COMPILE PLAN`).
2. Préproduction : exécution en parallèle sur des topics `.flink` pendant au moins un cycle métier complet (jours ouvrés, week-end, changements d'heure si les fenêtres horaires sont concernées).
3. Bascule par lots, des plus simples aux plus riches en état : 1 → 2 → 3/4 → 5 → 6 → 7/8.
4. Arrêt des requêtes ksqlDB d'un lot après validation des consommateurs ; décommissionnement de ksqlDB avant février 2027.

## Sources

- [Confluent Platform — Supported Versions and Interoperability (7.9)](https://docs.confluent.io/platform/7.9/installation/versions-interoperability.html)
- [ksqlDB — Legacy Documentation](https://docs.confluent.io/platform/current/ksqldb/operate-and-deploy/installation/legacy-documentation.html)
- [Apache Flink 1.20 — Savepoints](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/savepoints/)
