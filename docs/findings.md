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