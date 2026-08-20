"""Contenus éditoriaux publics du guide trail.

Ils sont volontairement rédigés comme des points de départ utiles et non comme
des fiches de résultats ou des règlements officiels. Les pages parcours sont
complétées au runtime par les données GPX locales.
"""

SEO_GUIDES = {
    "plan-de-course-trail": {
        "title": "Plan de course trail : créer une stratégie d’allure et de ravitaillement",
        "description": "Méthode pour construire un plan de course trail : lire le profil, répartir l’effort, anticiper les ravitos et ajuster son pacing.",
        "kicker": "Guide de préparation trail",
        "intro": "Un plan de course utile ne promet pas un chrono magique. Il transforme un parcours, tes allures et tes habitudes de ravitaillement en repères concrets : effort en montée, temps de passage, arrêts et marges de sécurité.",
        "sections": [
            ["Partir du terrain, pas d’une allure moyenne", "Découpe la course selon le relief et les points de ravitaillement. Sur un trail, une même allure moyenne peut masquer une longue montée marchée, une descente technique et plusieurs minutes d’arrêt."],
            ["Choisir des repères d’effort", "Associe une allure ou une VAM réaliste à chaque pente. Les premières heures doivent rester contrôlées : un plan sert autant à freiner au départ qu’à accélérer au bon moment."],
            ["Prévoir les ravitos", "Indique ce que tu prends, ce que tu remplis et le temps maximal d’arrêt. Les ravitos sont des transitions du plan : l’objectif est de repartir préparé pour le tronçon suivant."],
        ],
        "faq": [["Un plan de course doit-il être suivi à la minute ?", "Non. Il sert de cadre : adapte-le à la météo, au terrain, à ton état digestif et à tes sensations."], ["Peut-on faire un plan sans historique ?", "Oui, avec des allures prudentes de départ, puis en les corrigeant après les premières sorties." ]],
    },
    "pacing-trail": {
        "title": "Pacing trail : gérer son effort du départ à l’arrivée",
        "description": "Construire un pacing trail réaliste : intensité de départ, gestion des montées et descentes, temps de passage et marge pour les ravitos.",
        "kicker": "Stratégie d’allure",
        "intro": "Le pacing en trail consiste à répartir une réserve d’énergie limitée sur un relief irrégulier. La meilleure stratégie est rarement de courir vite au départ : elle garde de la disponibilité pour les portions qui comptent vraiment.",
        "sections": [["Les trois questions du départ", "À quel effort puis-je tenir plusieurs heures ? Où le parcours impose-t-il de marcher ? Quelle marge dois-je garder pour les ravitos, la chaleur ou la nuit ?"], ["Monter sans se mettre dans le rouge", "Sur une pente soutenue, alterner marche active et course peut être plus rentable qu’essayer de conserver une foulée coûteuse. Regarde la VAM et le ressenti, pas seulement l’allure au kilomètre."], ["Descendre avec lucidité", "La vitesse en descente dépend autant de la technique et de la fatigue musculaire que du cardio. Prévois une baisse de précision lorsque les heures s’accumulent."]],
        "faq": [["Faut-il viser un negative split en trail ?", "Parfois, mais le profil décide. Cherche surtout une deuxième partie maîtrisée plutôt qu’un départ trop ambitieux."], ["Quelle donnée suivre ?", "L’effort perçu, la fréquence cardiaque si elle est fiable, la pente et les temps de passage sont complémentaires."]],
    },
    "allure-trail-selon-pente": {
        "title": "Allure trail selon la pente : utiliser VAM, marche et descente",
        "description": "Comprendre comment adapter son allure trail à la pente : vitesse ascensionnelle, plat roulant, descente technique et profils GPX.",
        "kicker": "Allures et dénivelé",
        "intro": "Une allure sur plat ne se transpose pas directement à une pente de 15 %. Pour construire une projection crédible, il faut regarder comment tu avances réellement en montée, sur le roulant et en descente.",
        "sections": [["En montée : parler en VAM", "La vitesse ascensionnelle moyenne relie dénivelé et temps. Elle donne un repère plus lisible qu’une allure/km sur les pentes soutenues."], ["Sur le roulant : protéger l’économie", "Le plat et les faux plats sont les endroits où l’on peut gagner du temps sans exploser, à condition de rester dans une intensité durable."], ["En descente : intégrer la technicité", "Le terrain, l’adhérence et ta pratique comptent. Une projection doit rester prudente si la descente est longue ou très technique."]],
        "faq": [["Pourquoi mon allure devient-elle très lente en montée ?", "L’allure/km inclut la pente. Compare plutôt tes montées entre elles avec la pente, la VAM et l’intensité."], ["Le GPX suffit-il à prédire l’allure ?", "Non : il décrit le relief, mais pas toute la technicité ni les conditions du jour."]],
    },
    "ravitaillement-ultra-trail": {
        "title": "Ravitaillement ultra-trail : préparer ses arrêts et ses apports",
        "description": "Préparer les ravitos d’un ultra-trail : temps d’arrêt, glucides, boisson et organisation entre deux points de course.",
        "kicker": "Nutrition de course",
        "intro": "Un ravitaillement efficace commence avant d’y arriver. Le plan relie chaque point à la durée et au relief du tronçon suivant pour éviter de repartir sans boisson ou sans apport accessible.",
        "sections": [["Raisonner par tronçon", "Calcule ce que tu dois emporter jusqu’au prochain ravito : durée estimée, chaleur, dénivelé et tolérance digestive changent le besoin."], ["Limiter le temps perdu", "Prépare une liste simple : remplir, manger, prendre l’équipement utile, puis repartir. Les longues décisions se prennent avant la course."], ["Tester à l’entraînement", "Les quantités et aliments tolérés sont personnels. Un plan numérique reste un support de préparation, pas un avis médical."]],
        "faq": [["Faut-il s’arrêter à tous les ravitos ?", "Non. Arrête-toi quand le plan le justifie, mais ne saute pas une occasion essentielle de boire ou de recharger."], ["Puis-je changer le plan le jour J ?", "Oui, surtout selon la chaleur, l’appétit et les produits réellement disponibles."]],
    },
    "barrieres-horaires-trail": {
        "title": "Barrières horaires trail : calculer des temps de passage utiles",
        "description": "Anticiper les barrières horaires en trail : temps de passage, marges de sécurité, ravitos et stratégie de course.",
        "kicker": "Temps de passage",
        "intro": "Une barrière horaire est un repère opérationnel. Pour la gérer, il faut connaître son avance réelle à chaque contrôle et éviter de confondre vitesse moyenne globale et difficulté du tronçon suivant.",
        "sections": [["Lire la marge au bon endroit", "Compare ton heure de passage à la barrière, puis regarde la durée et le dénivelé à venir. Une avance confortable avant une grande montée peut disparaître rapidement."], ["Inclure les arrêts", "Les minutes de ravito font partie du chrono. Les prévoir aide à éviter de les subir."], ["Préparer un plan B", "Identifie les points où réduire l’arrêt, adapter l’équipement ou réévaluer la stratégie sans prendre de décision précipitée."]],
        "faq": [["Une marge de 10 minutes suffit-elle ?", "Cela dépend du prochain tronçon. Plus il est long, technique ou exposé, plus la marge nécessaire augmente."], ["Les barrières officielles changent-elles ?", "Elles peuvent évoluer : vérifie toujours les documents de l’organisateur avant le départ."]],
    },
    "utmb-mont-blanc": {
        "title": "UTMB Mont-Blanc : préparer son plan de course et son pacing",
        "description": "Guide de préparation des courses UTMB Mont-Blanc : plan d’allure, dénivelé, ravitaillements, barrières et projection à partir du GPX.",
        "kicker": "Événement UTMB Mont-Blanc",
        "intro": "Les parcours UTMB Mont-Blanc alternent longues ascensions, descentes exigeantes et ravitaillements décisifs. Un plan pertinent se construit course par course, avec les données officielles et le profil réellement parcouru.",
        "sections": [["Choisir la bonne course", "UTMB, CCC, TDS, OCC, MCC et ETC n’imposent ni les mêmes durées ni les mêmes compromis. Commence par sélectionner le parcours exact."], ["Découper autour des ravitos", "Chaque point officiel devient une étape : distance, dénivelé, temps de passage et temps d’arrêt prévu."], ["Faire vivre la projection", "Utilise une première estimation pour préparer, puis mets-la à jour après tes sorties spécifiques et la publication des dernières informations officielles."]],
        "faq": [["Les données affichées sont-elles officielles ?", "Les parcours sont documentés à partir des fichiers et informations disponibles ; l’organisateur reste la référence finale."], ["Puis-je préparer la CCC ou l’OCC ?", "Oui, les pages parcours donnent accès au simulateur associé quand le GPX est disponible."]],
        "course_ids": ["utmb-2026", "ccc-2026", "tds-2026", "occ-2026", "mcc-2026", "etc-2026"],
    },
    "grand-raid-des-pyrenees": {
        "title": "Grand Raid des Pyrénées : plan de course, allure et dénivelé",
        "description": "Préparer une course du Grand Raid des Pyrénées : analyser le GPX, établir les passages, gérer le dénivelé et les ravitaillements.",
        "kicker": "Événement Grand Raid des Pyrénées",
        "intro": "Dans les Pyrénées, la durée des montées, l’altitude et l’enchaînement des descentes demandent une stratégie posée. Le plan de course sert à transformer le relief en étapes compréhensibles.",
        "sections": [["Projeter par tronçon", "Découpe la course entre les points officiels pour visualiser la distance, le D+ et le temps estimé de chaque étape."], ["Conserver une marge", "Les changements de météo et la fatigue pèsent sur les parcours de montagne. Une projection prudente est plus utile qu’un chrono optimiste."], ["Préparer la logistique", "Les ravitos et assistances doivent être intégrés au même titre que l’allure : ce sont des points clés de la stratégie."]],
        "faq": [["Puis-je utiliser mon allure route ?", "Elle donne une base sur le roulant, mais la montée et la descente nécessitent des repères propres au trail."], ["Le GPX remplace-t-il les documents officiels ?", "Non. Vérifie parcours, barrières et consignes auprès de l’organisateur."]],
        "course_ids": ["grp-ultra-tour-2026", "grp-tour-des-cirques-2026", "grp-tour-des-lacs-2026", "grp-tour-du-moudang-2026", "grp-tour-du-bastan-2026", "grp-tour-de-la-gela-2026", "grp-tour-du-neouvielle-2026"],
    },
    "diagonale-des-fous": {
        "title": "Diagonale des Fous : construire une stratégie de pacing ultra-trail",
        "description": "Guide de préparation Diagonale des Fous : gestion de l’effort, dénivelé, nuit, ravitaillements et stratégie de course longue.",
        "kicker": "Ultra-trail à La Réunion",
        "intro": "La Diagonale des Fous demande une préparation de course longue où la gestion de la nuit, de la chaleur et du relief compte autant que le niveau de forme. Un plan doit rester adaptable aux conditions rencontrées.",
        "sections": [["Préparer les transitions", "La course se gagne souvent dans la continuité : alimentation, hydratation, équipement et gestion des arrêts doivent être pensés avant le départ."], ["Gérer les heures difficiles", "Prévois une stratégie de nuit : intensité plus prudente, alimentation accessible et décisions simples."], ["Rester fidèle à la source officielle", "Parcours, points de passage et règles évoluent selon l’édition ; vérifie-les toujours auprès de l’organisateur."]],
        "faq": [["Pourquoi préparer un plan flexible ?", "Parce que la météo et la fatigue peuvent modifier fortement les temps de passage."], ["Une projection remplace-t-elle l’expérience ?", "Non. Elle aide à préparer les décisions, mais ne remplace ni l’entraînement ni l’autonomie."]],
    },
    "festival-des-templiers": {
        "title": "Festival des Templiers : préparer son allure trail et son plan de course",
        "description": "Conseils de pacing et de préparation pour les courses du Festival des Templiers : relief, allures, descentes et ravitaillements.",
        "kicker": "Trail des Grands Causses",
        "intro": "Les courses du Festival des Templiers demandent de relier le profil du parcours à tes qualités de montée, de relance et de descente. Un plan simple permet de choisir les bons repères avant le départ.",
        "sections": [["Identifier les relances", "Le terrain vallonné récompense la régularité : évite de multiplier les accélérations coûteuses."], ["Préparer les descentes", "La vitesse dépend de la technicité et de l’état des jambes. Prévois une marge plutôt qu’une allure théorique parfaite."], ["Construire des étapes", "Utilise les ravitos comme limites naturelles pour répartir alimentation, hydratation et effort."]],
        "faq": [["Faut-il courir toutes les montées ?", "Non. La marche active peut être la stratégie la plus efficace sur les pentes soutenues."], ["Comment choisir son objectif ?", "Pars de sorties comparables et d’un rythme que tu peux répéter sur la durée."]],
    },
    "saintelyon": {
        "title": "SaintéLyon : plan d’allure pour un trail nocturne entre route et chemins",
        "description": "Préparer la SaintéLyon : stratégie d’allure nocturne, gestion des relances, équipement et ravitaillements.",
        "kicker": "Trail nocturne",
        "intro": "La SaintéLyon combine souvent portions roulantes, chemins, météo hivernale et course de nuit. La préparation du pacing doit tenir compte de la visibilité, de l’adhérence et de l’effort accumulé.",
        "sections": [["Gérer le départ rapide", "Les portions roulantes peuvent pousser à aller trop vite. Définis une intensité que tu peux conserver après plusieurs heures."], ["Anticiper la nuit", "Frontale, vêtements et alimentation doivent être accessibles sans perdre du temps ni se refroidir."], ["Adapter au sol", "Boue, froid ou terrain glissant modifient les allures. Le plan doit inclure une marge de sécurité."]],
        "faq": [["Faut-il viser une allure route ?", "Seulement sur les portions roulantes : les chemins et conditions nocturnes changent la dépense réelle."], ["Quelle est la priorité au ravito ?", "Boire, manger, ajuster l’équipement, puis repartir sans prolonger inutilement l’arrêt."]],
    },
}
