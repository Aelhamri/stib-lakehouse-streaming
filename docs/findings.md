# Constats sur les données

## Doublons apparents (contenu identique)
- 290 704 clés d'observation apparaissent plusieurs fois (308 951 lignes en trop, 3,35 % du total).
- 98 % ont `distanceFromPoint = 0`, et on trouve jusqu'à 6 copies.
- Hypothèse : plusieurs véhicules au même arrêt au même instant, pas des doublons réseau.
- Décision : Silver ne déduplique pas sur le contenu. La vraie déduplication se fera sur
  l'identité du message (étape streaming).

## Trous de collecte
- Un trou nocturne récurrent (environ 02:00 à 03:30 heure locale), ambigu : pause du collecteur ou absence de service.
- Pannes de journée le 6 mai (de 14:28 à 17:00 heure locale), micro-pertes d'un ou deux polls.
## Silver 1 : seuil de distance (EXTREME_DISTANCE)

- `distanceFromPoint` : distance en mètres le long de la ligne, par rapport à `pointId` (hypothèse non encore vérifiée).
- Seuil **provisoire** `MAX_DISTANCE_M = 5000`. Sur toutes les données, 2 839 lignes sont rejetées (2 799 sur la ligne 12, 40 sur la ligne 96). Je n'ai pas vérifié s'il existe un trou net dans la distribution de la ligne 12 entre 5 000 et ~10 500 m.
- Ces lignes sont en quarantaine, pas supprimées, et Bronze est intact : on peut les réintégrer.
- À reprendre au Silver 2 : projeter les arrêts sur `line_shapes` en Lambert 72 pour obtenir la vraie longueur de chaque tronçon et fixer un seuil par tronçon.

## Résultat Silver 1 (9 213 400 lignes en entrée)

- Valides : 9 001 973. Quarantaine : 211 427 (UNKNOWN_LINE 118 328, UNKNOWN_STOP 90 260, EXTREME_DISTANCE 2 839).