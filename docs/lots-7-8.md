# Lots 7 et 8 : fenetres et alarmes

Les cinq traitements du journal sont extraits par `python scripts/generate_lots78.py`.
`sql/ksqldb/07-08-windows.sql` conserve les requetes de reference et les contrats des
topics amont des lots 3 et 6. Le job Flink `lab.WindowJob` est configure par
`sql/flink/07-08-windows.json` ; toutes ses sorties portent le suffixe `.flink`.
Les deux alarmes ecrivent dans le meme topic, comme dans le journal.

Par defaut, les deux entrees lisent les topics historiques, afin de comparer les
moteurs sur les memes donnees amont. Pour une chaine entierement Flink, remplacer
`indicator_input` par `VA_PEAGE_BACKOFFICE_SOCLEET_FILTER_REF_INDICATEUR.flink`
(sortie du lot 6) et `flows` par `VA_PEAGE_CLASSIFICATION_FLOWS_EXTEND.flink`
(sortie du lot 3). Les champs `quality` et `count` servent a la fois de sorties et
d'entrees intermediaires ; ils doivent designer les memes topics dans un job complet.

```powershell
python scripts/generate_lots78.py
docker compose up -d --build --wait
python scripts/test_windows.py
docker compose exec -T jobmanager /opt/flink/bin/flink run -d -c lab.WindowJob /opt/flink/lib/migration-lab.jar --config /opt/flink/sql/07-08-windows.json
```

Creer les topics avant la soumission manuelle : six partitions, replication 1 dans
le laboratoire. Les topics horaires et sessions ont la politique `compact,delete`,
ceux des alarmes `compact`, retention historique sept jours. Ne pas ecraser les
topics ksqlDB pendant la comparaison. Les sources du job sont lues depuis le debut.

## Contrats conserves

* Lot 7 : regroupement sur six champs, dont l'identifiant provenant de la cle Kafka
  INTEGER et non d'un champ homonyme du JSON. Fenetres horaires UTC basees sur le
  timestamp Kafka, somme incrementale, valeurs absolues des projections historiques.
  Les colonnes de groupe restent dans la cle JSON ; elles ne sont pas ajoutees a la
  valeur. Une somme de valeurs toutes nulles demarre a zero dans la reference.
* Sessions : ecart d'inactivite de 60 minutes, bornes inclusives correspondant aux
  vrais timestamps des evenements. Les extensions et fusions retirent les anciennes
  cles avec des tombstones et emettent le nouvel agregat. Les resultats ne sont pas
  attendus jusqu'a la cloture. Le HAVING qualite conserve strictement ratio > 0.3 et
  nombre >= 20 ; les doublons comptent et les evenements TD/PostDTO sont filtres.
* Le temps est avance par partition de regroupement, avec le hachage Kafka Murmur2 et
  le nombre de partitions lu dans Kafka au demarrage (source de l'etape). Les anciennes API utilisees par ksqlDB 7.9.2 fixent la grace
  horaire et session a 24 heures moins la taille/le gap, soit 23 heures ici. Le
  nettoyage des sessions tient aussi compte du gap (24 heures au total). Les etats de
  fenetre expires sont purges par index, au passage de chaque heure de temps de flux ;
  la jointure des alarmes purge par minute et borne l'etat des cles inactives par un TTL de 48 h.
* Le comptage utilise `HyperLogLog(14)` de `ksqldb-stream-lib-7.9.2.jar`, extrait de
  l'image du moteur de reference. Le remplacer par un ensemble exact pourrait
  modifier le seuil des alarmes. Les conditions de distribution de cette dependance
  restent celles de son artifact et de l'image Confluent.
* Les cles binaires conservent le JSON ksqlDB suivi du debut de fenetre sur 8 octets
  pour l'horaire, ou de la fin puis du debut sur 16 octets pour les sessions.
  Le partitionnement des fenetres hache uniquement la cle metier. `SOCIETE` est
  uniquement dans la cle de l'alarme globale, tandis qu'elle est aussi une valeur
  dans l'alarme unitaire. Les accents des constantes SQL restent en UTF-8.
* Les deux sessions restent des topics physiques relus comme STREAM : leurs
  tombstones ne retractent pas les evenements deja consommes par les jointures.
* Les alarmes utilisent une jointure symetrique de +/- 60 secondes. Chaque paire
  correspondante alimente les agregats LATEST_BY_OFFSET, avec leurs seuils respectifs
  < 30 et >= 30. Les valeurs nulles des derniers champs sont ignorees. La conversion
  Europe/Paris conserve le suffixe Z literal du journal, meme en heure locale.

## Verification et perimetre

`scripts/test_windows.py` execute les cinq requetes sur les deux moteurs. Les quatre
frontieres sont testees separement (`hourly`, `quality`, `count`, `alarms`) pour fournir
aux jointures exactement le meme historique d'entree dans le meme ordre controle.
Un cinquieme scenario `pipeline` execute aussi les cinq requetes reliees par leurs
topics physiques, avec une unique paire de jointure pour lever l'ambiguite d'ordre.
Les SQL de reference par frontiere sont generes depuis les memes declarations.
Chaque phase attend les offsets committes des consommateurs et des repartitions.
Les rapports contiennent toutes les valeurs, les cles brutes base64, les partitions et les timestamps,
compares avec multiplicite. L'ordre des changements est aussi compare pour chaque
cle physique dans sa partition, afin de detecter une inversion mise a jour/suppression.
Les rapports sont conserves dans `artifacts/win*/comparison.json`.

Les scenarios couvrent les bornes horaires, les arrivees anciennes et expirees,
les doublons, les nulls, le franchissement des deux seuils qualite, les fusions de
sessions par un evenement intermediaire, le seuil de 30 voies, les jointures des
deux cotes, leurs limites temporelles et le changement d'heure Europe/Paris.

Preuves locales du 27 octobre 2026, build Docker du job Java 17 :

| Scenario | Sorties identiques par moteur | Rapport local |
| --- | ---: | --- |
| Horaire | 7 | `artifacts/winhourly0ecfd1ff/comparison.json` |
| Qualite | 17 | `artifacts/winquality79599734/comparison.json` |
| Comptage | 42 | `artifacts/wincountc06d29bd/comparison.json` |
| Deux alarmes | 21 | `artifacts/winalarmsd55a3c59/comparison.json` |
| Chaine complete, cas controle | 4 | `artifacts/winpipelined2b10204/comparison.json` |

Les rapports incluent les tombstones. L'ordre par cle a egalement ete verifie sur
ces cinq rapports. La CI rejoue ces scenarios avec les six scripts precedents.
La non-regression des helpers partages passe aussi : lot 5, 35 sorties par moteur
(`artifacts/badge561c6186`) ; lot 6, 26 sorties par moteur (`artifacts/socle4e90cf39`).

L'equivalence d'une chaine complete alimentee concurremment depend de l'ordre
d'arrivee entre les topics qualite et comptage. Kafka ne fournit pas d'ordre global
entre ces topics ; les deux moteurs peuvent produire des sequences et derniers
resultats differents selon cet entrelacement. Les tests par frontiere ne constituent
donc pas une certification d'isochronisme de la chaine concurrente en production.
Les caches et cadences de production doivent aussi etre alignes : le cache ksqlDB du
laboratoire est desactive pour observer les changements intermediaires.

Les garanties restent at-least-once. La restauration apres panne, la charge, les
politiques d'erreur JSON/coercition et une reprise sur historique incomplet ne sont
pas certifiees par ces tests. Le job conserve les etats dans les checkpoints Flink,
sans importer les RocksDB ksqlDB. Les sources doivent conserver suffisamment
d'historique pour reconstruire sessions et jointures (gap + grace au minimum).

## Bascule et retour arriere

1. Garder ksqlDB comme producteur de reference et lancer Flink sur les sorties
   `.flink`, avec un historique complet et un point d'offset documente.
2. Comparer en production les frontieres horaires, qualite, comptage et alarmes,
   en tenant compte de l'ordre des deux entrees de jointure. Rejouer aussi les
   incidents/reprises et mesurer le cout des etats avant validation de capacite.
3. Faire approuver toute difference liee a l'entrelacement avant de basculer les
   consommateurs ; ne pas presenter l'egalite d'etat final comme une preuve des
   changements intermediaires.
4. Conserver les consommateurs et topics de reference pendant la periode de
   retour arriere. Revenir aux topics ksqlDB si un ecart de contrat est constate.

Sources de semantique :
[fenetres et grace ksqlDB](https://docs.confluent.io/platform/current/ksqldb/concepts/time-and-windows-in-ksqldb-queries.html),
[constructeur des fenetres 7.9.2](https://github.com/confluentinc/ksql/blob/7.9.2-post/ksqldb-streams/src/main/java/io/confluent/ksql/execution/streams/StreamAggregateBuilder.java),
[TimeWindows Kafka 3.9](https://github.com/apache/kafka/blob/3.9.0/streams/src/main/java/org/apache/kafka/streams/kstream/TimeWindows.java),
[COUNT_DISTINCT de la branche 7.9.2](https://github.com/confluentinc/ksql/blob/7.9.2-post/ksqldb-engine/src/main/java/io/confluent/ksql/function/udaf/count/CountDistinct.java).
