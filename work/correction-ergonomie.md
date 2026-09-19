# Correction de l’ergonomie — 19 septembre 2026

La structure précédente (`ac4a65f`) redevient la référence : navigation Planifier / Activités / Compte / Mes données, accès direct au plan et présentation des activités avec leurs onglets habituels. Le nouvel accueil, le menu Énergie, le simulateur et les projections kcal ajoutées au plan sont supprimés.

Les kcal sont des indicateurs de sorties enregistrées : petite tuile dans le résumé, répartition dans les trois cartes de terrain existantes, rappel discret dans la liste et le profil. Les groupes de la vue d’ensemble suivent ses bornes existantes (descente ≤ −5 %, montée ≥ +5 %). Ce sont des estimations de locomotion sur la durée couverte, pas des apports à consommer.

Le tableau de distribution glycémique devient une barre colorée avec durées et pourcentages. La courbe conserve les interruptions du capteur et montre la plage 70–180 mg/dL. Les aplats des graphiques de terrain et cardio sont plus légers. Les libellés sont raccourcis et les chiffres utilisent une typographie cohérente.

Conservés : allures modélisées communes au profil et au plan, médiane anonymisée de secours, ajustement uniforme au chrono cible, calculs glycémie pondérés par le temps, synthèses en cache et chargement différé des graphiques. La carte se charge à son apparition sans clic supplémentaire.

Les exemples d’enrichissement Strava restent dans le réglage existant, sous un détail dépliable. Aucune publication ni déploiement effectués.

Validation : 57 tests Python et 5 tests JavaScript de courbe/allure ; syntaxe des templates et scripts vérifiée. Contrôle visuel sur données fictives dans Safari : activité et glycémie sur ordinateur, liste et activité à 390 px. Les graphiques conservent les données manquantes (pas de VAM à zéro inventée).
