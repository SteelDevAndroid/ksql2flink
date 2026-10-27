# Conseils et guide opérationnel de migration

Ce guide accompagne le [rapport détaillé](rapport-migration-detaille.md).
Il prépare une migration progressive ; il n’exécute aucune modification de production.

## 1. Décisions à prendre avant de planifier la bascule

| Décision | Conseil | Livrable attendu |
| --- | --- | --- |
| Définition de l’iso | Définir les dimensions obligatoires : contenu, clé, timestamp, ordre, cadence et effets consommateurs | Contrat d’acceptation par famille |
| Concurrence | Décider si l’identité des entrelacements est exigée ; sinon définir précisément les différences admissibles | Arbitrage métier écrit pour lots 6 et 8 |
| Livraison | Qualifier les doublons après reprise ; décider si at-least-once est acceptable | Garantie de livraison testée |
| Historique | Déterminer si l’état peut être reconstruit sans perte depuis les topics disponibles | Dossier d’initialisation par état |
| Objectifs de service | Fixer débit, latence, retard maximal, perte admissible et temps de reprise | Objectifs mesurables approuvés |
| Acheminement | Choisir comment les consommateurs passent des topics historiques aux sorties Flink | Matrice producteur-topic-consommateur |

Si une identité stricte de toutes les sorties concurrentes est exigée, les tests
actuels ne suffisent pas. Il faudra établir un ordre reproductible aux frontières
concernées ou revoir le contrat avec le métier. Une modification du modèle temporel
constituerait une évolution de comportement à valider séparément.

## 2. Préparer un dossier par chaîne

Relever dans l’environnement cible les noms des topics et groupes, les partitions,
les formats de clé et valeur, les timestamps utilisés, les rétentions, la compaction,
les caches ksqlDB et les dépendances des consommateurs. Comparer les réglages
effectifs avec les manifests du dépôt, et consigner chaque différence.

Pour chaque topic, documenter qui l’écrit, qui le lit, quelle clé définit son ordre
et quel traitement reçoit un tombstone. La même suppression peut être propagée
par une projection, éliminée par un filtre ou interprétée comme une suppression de table.

Ne pas augmenter le nombre de partitions pendant la migration des lots 7/8 sans
adapter et requalifier le code : les calculs de partitionnement livrés ciblent six
partitions. Le cas Lexhistoric conserve une sortie à deux partitions.

Identifier également les adresses Kafka configurées dans le code et les SQL.
Le laboratoire emploie `kafka:9092` et ne fournit pas une configuration de production
paramétrable et sécurisée complète. La préparer est une action préalable au déploiement.

## 3. Reconstruire les états avant de traiter les nouveaux faits

| État | Point d’attention | Vérification minimale |
| --- | --- | --- |
| Dernière référence | Les suppressions et dernières valeurs doivent être cohérentes au point de départ | Comparer l’état matérialisé clé par clé |
| COUNT des références, lot 6 | La dernière valeur d’un topic compacté ne contient pas le nombre historique de mises à jour | Rejouer l’historique complet ou concevoir une initialisation explicitement validée |
| Sommes horaires, lot 7 | Un démarrage au milieu d’une fenêtre peut perdre ses premières contributions | Reconstruire les fenêtres encore modifiables |
| Sessions qualité, lot 8 | Il faut les événements permettant de reconstruire bornes, compteurs et dernières valeurs | Comparer sessions actives et mises à jour attendues |
| HyperLogLog, lot 8 | Une cardinalité seule ne remplace pas les registres de l’agrégat | Reconstruire le sketch depuis un historique suffisant |
| Buffers de jointure, lot 8 | Une paire peut dépendre d’un événement arrivé avant le démarrage | Reconstituer les deux côtés sur l’horizon nécessaire |

Gap et grâce donnent un premier horizon d’analyse, pas une règle universelle de
replay. Une session active peut durer bien plus longtemps que son gap d’une heure ;
les compteurs historiques du lot 6 ne sont pas bornés à cet horizon. Une rétention
de sept jours ne prouve donc pas automatiquement que tous les états sont reconstructibles.

Ne pas démarrer un job vide aux derniers offsets pour éviter les doublons : cela
perdrait l’état antérieur. Ne pas rejouer aveuglément depuis earliest dans un topic
métier actif : cela réémettrait des effets historiques.

Un savepoint Flink est un moyen de conserver et restaurer un état Flink compatible ;
il ne convertit pas automatiquement les états ksqlDB. Conserver les UID et tester
la compatibilité de restauration. Les règles de propriété des snapshots doivent
être respectées avant de supprimer leurs fichiers.
[Documentation Flink 1.20](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/savepoints/).

## 4. Constituer les preuves de préproduction

Rejouer les commandes suivantes dans un environnement de laboratoire dédié.
Elles créent des topics et des objets isolés de test ; elles ne constituent pas
des commandes de bascule de production.

```powershell
docker compose up -d --build --wait
python scripts/smoke.py
python scripts/test_lexhistoric.py
python scripts/test_conformite.py
python scripts/test_lots34.py
python scripts/test_badges.py
python scripts/test_socleet.py
python scripts/test_windows.py
```

Archiver les rapports en conservant la révision testée, les versions d’images,
les fixtures et les SQL effectifs. Les tests doivent ensuite être complétés par :

1. Un replay représentatif des schémas et cas réellement produits, avec accès aux
   données conforme aux règles du projet.
2. Des entrelacements explicites référence/transaction et qualité/comptage, avec
   retards, répétitions, tombstones et événements hors ordre.
3. Des pannes avant/après checkpoint et pendant l’écriture Kafka, puis une reprise
   depuis checkpoint et savepoint.
4. Une perte de TaskManager et une indisponibilité des services nécessaires à la
   reprise, dans un environnement autorisé pour ces essais.
5. Une charge représentative des pics, de la cardinalité et du rattrapage après retard.

Les résultats doivent distinguer les écarts de valeur, clé, timestamp, partition,
ordre, multiplicité et cadence. Ne pas masquer les doublons en dédupliquant le
rapport si les consommateurs peuvent recevoir ces mêmes doublons.

## 5. Exécuter en parallèle sur des sorties séparées

Conserver ksqlDB comme référence et démarrer Flink sur des topics miroirs.
Utiliser des groupes et identités de jobs distincts. Les deux moteurs ne doivent
pas se partager un groupe de consommation pour se répartir les données de comparaison.

Pour les lots sans état, comparer les résultats à partir d’un ensemble d’entrées
identifiable. Pour les lots à état, attendre la reconstruction et le rattrapage
avant de tirer une conclusion sur les nouveaux événements.

Pendant la comparaison, surveiller :

| Mesure | Ce qu’elle permet de décider |
| --- | --- |
| Retard source par partition | Si les deux moteurs comparent la même frontière d’entrée |
| Retard des topics intermédiaires | Si les références et agrégats sont prêts pour les jointures |
| Écarts de contrat par catégorie | Si la divergence est sémantique, temporelle ou liée au replay |
| Rejets et erreurs de désérialisation | Si des données sont perdues ou bloquent un moteur |
| Nombre de sorties et tombstones | Si les changements intermédiaires sont conservés |
| Durée, succès et âge des checkpoints | Si une reprise exploitable existe |
| Taille d’état et ressources | Si la chaîne tient la charge et le rattrapage |
| Effets chez les consommateurs de contrôle | Si le contrat est utilisable de bout en bout |

Fixer les seuils selon les objectifs du projet avant l’observation, plutôt que de
les ajuster après constatation des résultats. Une durée de comparaison doit couvrir
les cycles métier, fenêtres et retards pertinents ; aucune durée fixe n’est réputée suffisante ici.

## 6. Préparer puis répéter la bascule

La procédure la plus facile à contrôler consiste à conserver les sorties isolées
et à basculer les consommateurs sur une frontière métier documentée. Elle exige
une stratégie de positionnement propre à chaque consommateur. Si le nom du topic
historique doit rester inchangé, préparer une procédure dédiée pour assurer
l’unicité du producteur et la continuité de ses effets.

Procédure proposée, à répéter en préproduction :

1. Figer les configurations, la révision et les images validées. Enregistrer les
   topics, jobs, groupes et consommateurs concernés.
2. Choisir une frontière d’entrée par partition, notée F, et un mécanisme de
   clôture des effets avant F : pause autorisée, barrière applicative ou protocole équivalent.
   Une simple lecture instantanée des offsets ne crée pas cette clôture.
3. Prouver que l’état Flink correspond aux événements strictement antérieurs à F,
   ou documenter la frontière exacte différente retenue. Prévoir la reprise à F
   avec cet état ; ne pas associer des offsets nouveaux à un état vide ou ancien.
4. Attendre le rattrapage des références, sessions et buffers nécessaires. Vérifier
   les changements intermédiaires et les effets consommateurs jusqu’à la frontière.
5. Pour chaque consommateur, définir quelles sorties sont déjà appliquées et le
   premier événement encore à appliquer. Les offsets des topics ksqlDB et Flink
   ne sont pas interchangeables ; utiliser des marqueurs métier ou une réconciliation validée.
6. Effectuer le changement d’acheminement prévu, puis reprendre le traitement.
   Vérifier qu’un seul chemin applique les effets métier.
7. Contrôler les écarts et les objectifs de service. Conserver la capacité de
   revenir en arrière jusqu’à la clôture de la période d’observation convenue.

Lorsque des effets externes non idempotents existent, leur journal de traitement
et leur stratégie de compensation font partie de cette répétition. L’égalité des
topics seule ne prouve pas l’absence de double application chez ces consommateurs.

## 7. Rendre le retour arrière réellement opérable

Préparer le retour avant la bascule. Conserver les configurations ksqlDB, les
frontières d’offsets, les données nécessaires et l’état de référence exploitable.
Si ksqlDB continue en parallèle, empêcher que ses sorties provoquent une deuxième
application des effets métier tout en gardant sa capacité à servir de référence.

| Déclencheur | Action préparée |
| --- | --- |
| Écart de contrat non expliqué | Suspendre le chemin concerné à une frontière contrôlée et préserver les preuves |
| Retard ou instabilité au-delà des objectifs | Arrêter la progression de la bascule et évaluer le rattrapage |
| État incomplet ou reprise incorrecte | Restaurer un état prouvé ou revenir au chemin de référence selon le plan répété |
| Effets déjà appliqués en double ou incorrectement | Réconcilier ou compenser avec le consommateur avant reprise |

Revenir au topic historique ne suffit pas à lui seul : il faut tenir compte des
événements déjà appliqués après la bascule. Éviter le saut de données nouvelles
comme leur réapplication. Répéter ce cas avec des événements produits pendant
la période Flink, et conserver le résultat de réconciliation.

## 8. Recommandations propres aux lots

| Lot | Conseil prioritaire avant bascule |
| --- | --- |
| 1 | Valider les consommateurs avec clés nulles, sortie à deux partitions et premier paiement uniquement |
| 2 | Maintenir l’exclusion ARS entrée ; comparer les schémas imbriqués et les tombstones |
| 3 | Vérifier les clés INTEGER et le raccordement au lot 6 ; conserver les collisions de concaténation historiques |
| 4 | Conserver les topics exploded et les différences ASF/ESC ; vérifier le comportement des nulls dans les FAV |
| 5 | Tester des historiques distincts au sein d’un même message pour détecter toute perte de corrélation |
| 6 | Charger les références avant les faits ; prouver le COUNT historique ; tester l’absence de réémission rétroactive |
| 7 | Reconstituer les fenêtres ouvertes ; conserver le timestamp Kafka et la grâce effective de la version historique |
| 8 | Comparer sessions, tombstones et deux côtés des jointures ; tester 29/30/31 voies et l’ordre concurrent |

Ne pas combiner la migration avec une correction des dates, des collisions de clé,
du comptage approximatif ou des règles de nulls. Ces évolutions doivent avoir
leur propre demande, leur contrat attendu et leur validation métier.

## 9. Travaux proposés, par priorité

**Avant toute bascule générale :** qualifier les états initiaux, les entrelacements
des lots 6/8, les pannes et les consommateurs ; répéter bascule et retour arrière.

**Pour préparer le déploiement :** externaliser les paramètres dépendants du
laboratoire, préparer le stockage durable et la restauration, définir la politique
de rejet, puis instrumenter les écarts et les objectifs de service.

**Pour industrialiser ensuite :** automatiser l’archivage des preuves, ajouter les
replays représentatifs à une qualification régulière et documenter les changements
de version des fonctions de compatibilité et de la bibliothèque HyperLogLog.

Ces travaux sont des recommandations de suite ; ils ne sont pas présentés comme
des fonctionnalités déjà livrées ou des tests déjà exécutés.
