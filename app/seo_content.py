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
    "sport-diabete-type-1": {
        "title": "Sport et diabète de type 1 : préparer son activité physique en sécurité",
        "description": "Sport et diabète de type 1 : comprendre les effets possibles de l’effort sur la glycémie, préparer sa séance et savoir quand demander conseil.",
        "kicker": "Diabète de type 1 et activité physique",
        "intro": "Le sport est compatible avec le diabète de type 1. Sa préparation demande toutefois un cadre personnel : type d’effort, insuline active, alimentation, données de glycémie, matériel et environnement ne produisent pas la même réponse d’une séance à l’autre.",
        "medical_notice": "Cette page est une information générale, pas un protocole de soins. Les objectifs glycémiques, apports et adaptations d’insuline doivent être définis avec l’équipe soignante qui connaît votre traitement et vos antécédents.",
        "sections": [
            ["Pourquoi l’effort ne fait pas toujours varier la glycémie dans le même sens", "Une sortie d’endurance, des intervalles, une compétition, le stress, la chaleur ou l’insuline encore active peuvent produire des réponses différentes. Observer ses propres tendances permet de préparer un plan plus sûr, sans extrapoler la séance d’un autre sportif."],
            ["Préparer avant de partir", "Prévoyez le moyen de contrôler votre glycémie, des glucides à action rapide, de l’eau, votre traitement habituel et une information médicale accessible. Pour une activité isolée, prévenez aussi une personne de l’itinéraire et de ce qu’elle doit faire en cas de problème."],
            ["Surveiller pendant et après", "Le suivi ne s’arrête pas à la fin de l’effort : une hypoglycémie peut aussi être retardée. Les capteurs sont utiles pour lire une tendance, mais tout symptôme ou valeur inattendue mérite d’être vérifié selon les consignes de l’équipe soignante."],
            ["Construire un plan personnel avec les soignants", "Conservez le contexte de vos séances : durée, intensité, repas, insuline active, valeurs et symptômes. Ce journal aide le diabétologue ou l’éducateur à définir des ajustements individualisés et à sécuriser les sorties plus longues."],
        ],
        "faq": [["Peut-on faire du sport avec un diabète de type 1 ?", "Oui, l’activité physique est bénéfique, avec une préparation et un suivi adaptés à la personne."], ["Faut-il modifier son traitement en suivant un guide en ligne ?", "Non. Les modifications d’insuline et d’apports doivent être décidées avec les professionnels qui suivent votre diabète."]],
        "sources": [["Fédération Française des Diabétiques — activité physique et diabète", "https://www.federationdesdiabetiques.org/information/sport-activite-physique"], ["American Diabetes Association — activité physique et diabète", "https://diabetesjournals.org/care/article/39/11/2065/37249/Physical-Activity-Exercise-and-Diabetes-A-Position"]],
    },
    "glycemie-et-sport": {
        "title": "Glycémie et sport : comprendre les variations pendant l’effort",
        "description": "Comprendre les variations de glycémie avant, pendant et après le sport avec un diabète de type 1 : intensité, durée, capteur et suivi personnalisé.",
        "kicker": "Glycémie pendant le sport",
        "intro": "La glycémie pendant le sport dépend de bien plus que de l’activité elle-même. L’intensité, la durée, le moment du repas, l’insuline active, le sommeil et le stress peuvent modifier la réponse. L’enjeu est de repérer des tendances personnelles, pas de chercher une règle universelle.",
        "medical_notice": "Les données de glycémie ne remplacent pas l’évaluation de votre état. En cas de symptôme, de doute ou de valeur inattendue, suivez votre plan de soins et demandez un avis médical si nécessaire.",
        "sections": [
            ["Endurance, intensité et réponse individuelle", "L’effort prolongé d’intensité modérée peut favoriser une baisse de glycémie, tandis qu’un effort très intense ou compétitif peut parfois s’accompagner d’une hausse transitoire. Ces mécanismes ne permettent pas de prédire précisément la réponse d’une personne sans son propre historique."],
            ["Lire une tendance plutôt qu’un chiffre isolé", "Un chiffre de capteur est plus utile avec sa flèche de tendance, le moment de la dernière prise alimentaire, l’effort déjà réalisé et les symptômes. Les mesures interstitielles peuvent aussi avoir un décalage par rapport au sang lors de changements rapides."],
            ["Après la séance : une période à anticiper", "Le retour au calme et les heures suivantes font partie de la séance. Notez les baisses ou hausses tardives pour les discuter avec votre équipe de diabétologie, notamment avant une sortie longue, une course ou un entraînement nocturne."],
            ["Créer des repères fiables", "Répéter un format d’entraînement comparable, consigner son contexte et réviser les données avec un professionnel permet de transformer une observation en stratégie personnelle. Les applications et graphiques aident à visualiser ; ils ne prescrivent pas un traitement."],
        ],
        "faq": [["Pourquoi ma glycémie augmente-t-elle parfois pendant le sport ?", "L’intensité élevée, le stress et les hormones de l’effort peuvent notamment contribuer à une hausse transitoire."], ["Un capteur suffit-il pour décider ?", "Il apporte une information précieuse, à interpréter avec les symptômes, la tendance et les consignes médicales personnelles."]],
        "sources": [["Diabetes UK — exercise and diabetes", "https://www.diabetes.org.uk/living-with-diabetes/exercise"], ["American Diabetes Association — variabilité glycémique et activité physique", "https://diabetesjournals.org/care/article/39/11/2065/37249/Physical-Activity-Exercise-and-Diabetes-A-Position"]],
    },
    "hypoglycemie-pendant-sport": {
        "title": "Hypoglycémie pendant le sport : reconnaître, prévenir et réagir",
        "description": "Hypoglycémie et activité physique avec un diabète de type 1 : signes possibles, matériel à emporter, surveillance et plan d’action personnalisé.",
        "kicker": "Hypoglycémie et effort",
        "intro": "L’hypoglycémie peut survenir pendant l’exercice, après l’effort ou plus tard. La prévention repose sur l’anticipation, un accès immédiat aux glucides rapides et un plan d’action connu à l’avance par le sportif et, si besoin, ses proches ou partenaires d’entraînement.",
        "medical_notice": "Une hypoglycémie sévère ou des troubles de la conscience constituent une urgence. Cette page ne remplace pas votre plan de prise en charge ni l’apprentissage du glucagon avec votre entourage et votre équipe soignante.",
        "sections": [
            ["Repérer les signaux sans les banaliser", "Tremblements, sueurs, faim, pâleur, fatigue soudaine, difficultés de concentration ou comportement inhabituel peuvent faire évoquer une hypoglycémie. Les symptômes peuvent varier selon les personnes et être moins perceptibles après des épisodes répétés."],
            ["Toujours garder de quoi se resucrer", "Avant une séance, vérifiez que des glucides rapides sont accessibles sans fouiller un sac et que votre accompagnant sait où les trouver. Sur trail ou vélo, répartissez-les entre poches et sac afin de ne pas perdre l’accès au matériel en cas de chute ou de séparation."],
            ["Arrêter et suivre son protocole personnel", "Face à une suspicion d’hypoglycémie, interrompez l’effort et appliquez le protocole convenu avec vos soignants. Ne reprenez pas seul ou trop tôt : la priorité est de retrouver un état stable et de comprendre le contexte de l’épisode."],
            ["Tenir compte du risque tardif", "Une baisse peut apparaître plusieurs heures après l’activité. Le suivi post-effort, y compris la nuit lorsque cela est pertinent pour vous, doit faire partie du plan discuté avec l’équipe médicale."],
        ],
        "faq": [["Puis-je continuer à courir si je me sens en hypo ?", "Non. Interrompez l’effort et appliquez votre plan d’action personnel avant toute reprise."], ["Dois-je emporter du glucagon ?", "Discutez de la prescription, de son transport et de la formation de vos proches avec votre équipe soignante."]],
        "sources": [["Fédération Française des Diabétiques — sport, hypo et hyperglycémie", "https://www.federationdesdiabetiques.org/diabete/activite-physique/activite-physique-pense-bete"], ["NHS — hypoglycaemia (low blood sugar)", "https://www.nhs.uk/conditions/low-blood-sugar-hypoglycaemia/"]],
    },
    "hyperglycemie-et-sport": {
        "title": "Hyperglycémie et sport : savoir quand adapter ou différer l’effort",
        "description": "Hyperglycémie avant ou pendant le sport avec un diabète de type 1 : comprendre les situations à risque, les cétones et l’importance d’un plan médical personnel.",
        "kicker": "Hyperglycémie et activité physique",
        "intro": "Une hyperglycémie ne se gère pas uniquement en regardant un nombre : le contexte, les symptômes, l’intensité prévue et la présence éventuelle de cétones comptent. Pour le diabète de type 1, il faut disposer d’un protocole personnel clair avant les entraînements ou compétitions.",
        "medical_notice": "En présence de cétones, de vomissements, de douleur abdominale, de respiration inhabituelle ou de malaise, ne poursuivez pas l’effort et suivez sans délai les consignes d’urgence de votre équipe soignante. Cette page ne donne pas de dose de correction.",
        "sections": [
            ["Pourquoi le sport peut parfois faire monter la glycémie", "L’effort très intense, le stress de compétition ou une maladie peuvent favoriser une hausse. À l’inverse, une activité modérée n’a pas la même réponse. L’interprétation dépend également de l’insuline disponible dans l’organisme."],
            ["Cétones : un signal à prendre au sérieux", "En cas d’hyperglycémie importante, les recommandations médicales peuvent prévoir une recherche de cétones. Une présence de cétones modérées ou élevées est une situation où l’exercice est à éviter et où le protocole de soins doit guider la conduite à tenir."],
            ["Ne pas improviser une correction", "La dose, le délai d’action et la surveillance dépendent du schéma de traitement. Les ajustements se préparent avec le diabétologue ou l’infirmier d’éducation thérapeutique, en tenant compte de vos données et de votre activité."],
            ["Prévoir une solution de repli", "Pour une sortie éloignée, définissez à l’avance les critères qui imposent de renoncer, un moyen de rentrer et une personne à contacter. Un plan de sécurité rend la décision de différer une séance plus simple et plus responsable."],
        ],
        "faq": [["Puis-je faire du sport avec des cétones ?", "Non : une présence modérée ou élevée de cétones est un motif pour ne pas faire d’exercice et appliquer rapidement le protocole médical."], ["Une hyperglycémie après des intervalles est-elle possible ?", "Oui, une activité intense peut s’accompagner d’une hausse transitoire ; il faut l’interpréter avec votre protocole personnalisé."]],
        "sources": [["Fédération Française des Diabétiques — conduite à tenir lors d’une hyperglycémie", "https://www.federationdesdiabetiques.org/diabete/activite-physique/activite-physique-pense-bete"], ["American Diabetes Association — activité physique et cétones", "https://diabetesjournals.org/care/article/39/11/2065/37249/Physical-Activity-Exercise-and-Diabetes-A-Position"]],
    },
    "course-a-pied-diabete-type-1": {
        "title": "Course à pied et diabète de type 1 : organiser ses sorties avec sérénité",
        "description": "Course à pied et diabète de type 1 : préparer entraînement, matériel, glycémie, sécurité et retour d’expérience sans remplacer le suivi médical.",
        "kicker": "Running et diabète de type 1",
        "intro": "Une sortie de course à pied se prépare comme une sortie longue : itinéraire, durée, météo, ravitaillement et sécurité. Avec un diabète de type 1, ces éléments s’ajoutent au suivi glycémique et au protocole individuel construit avec les soignants.",
        "medical_notice": "Le contenu est éducatif. Toute adaptation de l’insuline, des glucides ou des alarmes de capteur doit être validée avec votre équipe de diabétologie.",
        "sections": [
            ["Choisir un format simple pour apprendre", "Pour découvrir votre réponse à la course, commencez par des sorties prévisibles, proches de chez vous et faciles à interrompre. Une fois des tendances identifiées avec l’équipe soignante, vous pourrez les tester progressivement sur des formats plus longs."],
            ["Composer un kit de sortie", "Préparez le matériel médical prescrit, un moyen de contrôle de la glycémie, des glucides rapides, de l’eau, un téléphone chargé et une identification indiquant le diabète. Vérifiez la tenue du capteur et du dispositif d’administration selon la transpiration et les frottements."],
            ["Ne pas courir isolé sans filet", "Partagez votre parcours, votre heure de retour prévue et les informations utiles à une personne de confiance. Sur une sortie de groupe, expliquez simplement ce qui peut vous arriver et où se trouvent vos réserves de secours."],
            ["Faire du retour d’expérience une force", "Après la séance, consignez ce qui a influencé la glycémie : dénivelé, allure, repas, chaleur, sommeil, insuline active et éventuels symptômes. Ces notes permettent des échanges bien plus utiles avec les soignants."],
        ],
        "faq": [["Puis-je utiliser mes données de course pour mieux comprendre ma glycémie ?", "Oui, relier les données d’activité au contexte glycémique peut aider à repérer des tendances, sans remplacer l’interprétation médicale."], ["Que prévoir sur une sortie longue ?", "Un plan d’itinéraire, du matériel de sécurité et le protocole établi avec votre équipe pour la durée et l’isolement envisagés."]],
        "sources": [["Diabetes UK — conseils généraux sur l’exercice avec diabète", "https://www.diabetes.org.uk/living-with-diabetes/exercise"], ["Fédération Française des Diabétiques — activité physique et diabète", "https://www.federationdesdiabetiques.org/information/sport-activite-physique"]],
    },
    "trail-diabete-type-1": {
        "title": "Trail et diabète de type 1 : préparer une sortie longue ou une course",
        "description": "Trail et diabète de type 1 : anticiper l’isolement, le dénivelé, la durée, la glycémie, le matériel de secours et un plan de sécurité personnalisé.",
        "kicker": "Trail, autonomie et diabète de type 1",
        "intro": "En trail, l’éloignement, le dénivelé, la météo et la durée rendent la préparation plus exigeante. Le but n’est pas de contrôler chaque imprévu, mais de disposer d’un plan de sécurité clair, d’un matériel accessible et de décisions anticipées avec l’équipe soignante.",
        "medical_notice": "Une course ou sortie isolée demande une préparation individualisée. Avant un format long, discutez du projet avec votre équipe de diabétologie ; ne modifiez jamais seul votre traitement sur la base de cette page.",
        "sections": [
            ["Découper la sortie en points de décision", "Ravitos, refuges, accès routiers et points hauts sont autant d’occasions de réévaluer la météo, l’état général, le matériel et les données glycémiques. Préparez à l’avance les options de raccourci ou d’abandon."],
            ["Doubler les éléments essentiels", "Sur un parcours de montagne, répartissez les glucides rapides, le matériel de suivi et les moyens de communication. Un élément unique au fond du sac n’est pas une sécurité suffisante s’il est inaccessible, perdu ou exposé à la pluie."],
            ["Informer sans compliquer", "L’accompagnant ou l’organisation n’a pas besoin de connaître tous les détails médicaux, mais doit savoir que vous vivez avec un diabète, reconnaître une situation inquiétante, trouver les informations d’urgence et appeler les secours si nécessaire."],
            ["Faire primer la sécurité sur le classement", "Météo, symptômes, cétones, matériel défaillant ou impossibilité de se resucrer sont des raisons légitimes de ralentir, s’arrêter ou renoncer. La meilleure stratégie de course est celle qui préserve une marge de sécurité réelle."],
        ],
        "faq": [["Un plan de course peut-il inclure la glycémie ?", "Il peut servir de carnet de préparation et de suivi, mais les décisions thérapeutiques doivent rester issues du protocole défini avec vos soignants."], ["Dois-je annoncer mon diabète à l’organisation ?", "Vérifiez les modalités de la course et discutez avec votre équipe de l’information utile à partager en cas d’urgence."]],
        "sources": [["Fédération Française des Diabétiques — pense-bête sport et glycémie", "https://www.federationdesdiabetiques.org/diabete/activite-physique/activite-physique-pense-bete"], ["American Diabetes Association — recommandations sur l’exercice et le diabète", "https://diabetesjournals.org/care/article/39/11/2065/37249/Physical-Activity-Exercise-and-Diabetes-A-Position"]],
    },
    "trail-et-glycemie": {
        "title": "Trail et glycémie : suivre les variations sur le profil du parcours",
        "description": "Comment rapprocher glycémie, dénivelé, cardio, allure et VAM pendant un trail avec un capteur de glucose et les données Strava.",
        "keywords": "trail glycémie, capteur glycémie trail, glucose trail, CGM trail, dénivelé glycémie",
        "kicker": "Analyse glycémique du trail",
        "intro": "En trail, le relief et les changements d’intensité rendent une courbe isolée difficile à interpréter. Synchroniser la glycémie avec la trace permet de replacer chaque mesure sur une montée, une descente ou une relance.",
        "medical_notice": "Cette analyse rétrospective aide à observer des tendances personnelles. Elle n’est ni une alarme médicale ni une recommandation de traitement.",
        "sections": [["Lire le glucose sur le terrain", "Le profil altimétrique coloré par plages montre où une variation a été mesurée, sans prétendre en déterminer seul la cause."], ["Croiser cardio, allure et VAM", "Comparer les mêmes horodatages permet de voir le contexte d’effort associé à chaque mesure et de préparer des questions plus précises pour son équipe soignante."], ["Construire un historique personnel", "Des sorties comparables et correctement synchronisées sont plus utiles qu’une séance isolée pour repérer des tendances reproductibles."]],
        "faq": [["Peut-on colorer le profil du trail selon la glycémie ?", "Oui, si le parcours et les mesures CGM disposent d’horodatages compatibles."], ["Le graphique explique-t-il une hypo ou une hyper ?", "Non. Il montre une coïncidence temporelle à interpréter avec le contexte et un professionnel de santé."]],
        "tracking_cta": True,
        "sources": [["Fédération Française des Diabétiques — activité physique et glycémie", "https://www.federationdesdiabetiques.org/diabete/activite-physique/activite-physique-pense-bete"]],
    },
    "alimentation-glucides-trail": {
        "title": "Alimentation et glucides en trail : préparer, observer et ajuster",
        "description": "Alimentation et glucides en trail : préparer les apports, noter le contexte et rapprocher ravitaillement, effort et glycémie sans protocole universel.",
        "keywords": "alimentation glucides trail, nutrition trail glycémie, ravitaillement glucides trail, glucose course longue",
        "kicker": "Nutrition et effort long",
        "intro": "Les besoins et la tolérance digestive varient avec la durée, l’intensité, la météo et la personne. Un journal reliant apports, terrain et mesures aide à documenter ce qui s’est passé sans transformer une moyenne en prescription.",
        "medical_notice": "Les quantités de glucides et adaptations d’insuline sont individuelles. Définissez votre stratégie avec un professionnel de santé et testez-la dans le cadre convenu.",
        "sections": [["Préparer par tronçon", "Associer les ravitaillements au temps prévu jusqu’au point suivant rend le plan plus concret qu’une quantité globale."], ["Noter ce qui a réellement été consommé", "L’heure, le produit, la quantité et la tolérance donnent du contexte aux courbes d’effort et de glycémie."], ["Relire sans conclure trop vite", "Sommeil, stress, température et insuline active peuvent également intervenir ; plusieurs sorties comparables sont nécessaires pour dégager une tendance."]],
        "faq": [["Existe-t-il une quantité universelle de glucides ?", "Non. Elle dépend notamment de l’effort, de la tolérance et, en cas de diabète, du plan défini avec les soignants."], ["Le capteur remplace-t-il un plan nutritionnel ?", "Non, il ajoute une mesure à remettre dans son contexte."]],
        "tracking_cta": True,
        "sources": [["Fédération Française des Diabétiques — alimentation et activité physique", "https://www.federationdesdiabetiques.org/diabete/alimentation/alimentation-et-activite-physique"]],
    },
    "glycemie-velo": {
        "title": "Glycémie et vélo : suivre son effort sur route, gravel ou VTT",
        "description": "Suivi de la glycémie à vélo : synchroniser capteur, parcours, fréquence cardiaque, vitesse et dénivelé pour relire une sortie.",
        "keywords": "glycémie vélo, capteur glucose cyclisme, CGM vélo, diabète cyclisme, glucose VTT",
        "kicker": "Cyclisme et capteur glucose",
        "intro": "À vélo, la durée, les côtes et les changements de rythme peuvent être rapprochés des mesures du capteur pour relire la sortie kilomètre après kilomètre.",
        "medical_notice": "Ne consultez pas un écran et ne prenez pas de décision complexe en roulant. Arrêtez-vous en sécurité et suivez votre protocole personnel.",
        "sections": [["Replacer une mesure sur le parcours", "La trace indique si la valeur correspond à une ascension, une descente ou une portion régulière."], ["Comparer intensité et glycémie", "Cardio, vitesse et dénivelé ajoutent le contexte nécessaire à la courbe CGM."], ["Prévoir l’autonomie", "Sur une sortie longue ou isolée, matériel, glucides accessibles, batterie et solution de retour font partie de la préparation."]],
        "faq": [["Strava reçoit-il directement la glycémie ?", "Non. Running Data Plan rapproche séparément les données Strava et celles de la source CGM."], ["Le suivi fonctionne-t-il en VTT ?", "Oui si l’activité et les mesures disposent de données temporelles exploitables."]],
        "tracking_cta": True,
        "sources": [["American Diabetes Association — glucose et exercice", "https://diabetes.org/health-wellness/fitness/blood-glucose-and-exercise"]],
    },
    "glycemie-running": {
        "title": "Glycémie et running : analyser cardio, allure et parcours",
        "description": "Glycémie en running : rapprocher les données d’un capteur de glucose avec l’allure, le cardio et le profil d’une sortie Strava.",
        "keywords": "glycémie running, glycémie course à pied, capteur glucose running, CGM sport",
        "kicker": "Course à pied et glucose",
        "intro": "Synchroniser la courbe glycémique avec l’allure et la fréquence cardiaque permet de retrouver le contexte exact d’une variation pendant une sortie.",
        "medical_notice": "Les graphiques servent à la relecture sportive et ne remplacent ni les alarmes du dispositif ni les consignes de l’équipe soignante.",
        "sections": [["Une chronologie commune", "Les données sont rapprochées par leur heure afin de lire allure, cardio, altitude et glucose au même moment."], ["Comparer des séances similaires", "Deux footings ou deux séances de seuil comparables donnent davantage de contexte qu’un mélange de formats."], ["Documenter l’après-effort", "Certaines variations peuvent être retardées ; l’historique permet de conserver la séance et son contexte."]],
        "faq": [["Quels graphiques sont disponibles ?", "Profil du parcours, glycémie, fréquence cardiaque, zones cardio, allure et VAM selon les données de la sortie."], ["Est-ce du temps réel ?", "L’analyse dépend de la source et de la synchronisation ; elle ne constitue pas un système d’alerte en temps réel."]],
        "tracking_cta": True,
        "sources": [["Fédération Française des Diabétiques — sport et glycémie", "https://www.federationdesdiabetiques.org/diabete/activite-physique/activite-physique-pense-bete"]],
    },
    "glycemie-marathon": {
        "title": "Glycémie et marathon : préparer et relire 42,195 km",
        "description": "Glycémie pendant un marathon : documenter allure, cardio, ravitaillements et mesures CGM pour analyser la course avec son équipe soignante.",
        "keywords": "glycémie marathon, diabète marathon, capteur glucose marathon, glucides marathon diabète",
        "kicker": "Marathon et suivi glycémique",
        "intro": "Le marathon combine durée, fatigue, ravitaillements et évolution de l’allure. Une vue synchronisée aide à conserver une trace structurée de la course.",
        "medical_notice": "Préparez toute stratégie de course et toute adaptation thérapeutique avec votre équipe médicale bien avant le jour J.",
        "sections": [["Tester avant la course", "Les sorties longues servent à éprouver matériel, accès aux glucides et protocole personnel dans un cadre moins contraint."], ["Lire les passages clés", "Le graphique permet de rapprocher ravitaillements, changement d’allure, cardio et plages glycémiques."], ["Analyser après l’arrivée", "La relecture doit inclure les heures suivant l’effort, selon les recommandations personnelles reçues."]],
        "faq": [["Peut-on préparer un marathon avec ces graphiques ?", "Ils peuvent documenter les répétitions et aider au dialogue avec les soignants, mais ne prescrivent aucune stratégie médicale."], ["La glycémie vient-elle de Strava ?", "Non, elle vient du service CGM connecté puis est synchronisée avec l’activité."]],
        "tracking_cta": True,
        "sources": [["American Diabetes Association — exercice et diabète de type 1", "https://diabetes.org/health-wellness/fitness/exercise-and-type-1"]],
    },
    "glycemie-semi-marathon": {
        "title": "Glycémie et semi-marathon : analyser allure et intensité",
        "description": "Suivi glycémique sur semi-marathon : visualiser glucose, allure, fréquence cardiaque et parcours sur 21,1 km.",
        "keywords": "glycémie semi marathon, diabète semi marathon, glucose 21 km, CGM course à pied",
        "kicker": "21,1 km et glucose",
        "intro": "Sur semi-marathon, l’intensité est souvent plus régulière mais plus élevée qu’en sortie longue. Les courbes synchronisées permettent de revoir précisément l’effort.",
        "medical_notice": "Aucune couleur de graphique ne constitue à elle seule une consigne de poursuite, d’arrêt ou de traitement.",
        "sections": [["Préparer un scénario connu", "Répéter allure cible, matériel et protocole convenu limite les nouveautés le jour de la course."], ["Observer la dérive", "L’évolution conjointe de l’allure, du cardio et du glucose donne une vue plus complète de la seconde moitié."], ["Comparer avec l’entraînement", "Les séances spécifiques servent de points de comparaison à condition de conserver leur contexte."]],
        "faq": [["Peut-on voir la glycémie par kilomètre ?", "Les mesures peuvent être positionnées par distance lorsque la synchronisation temporelle avec la trace est disponible."], ["Les zones cardio sont-elles affichées ?", "Oui, si l’activité contient une fréquence cardiaque exploitable."]],
        "tracking_cta": True,
        "sources": [["American Diabetes Association — comprendre glucose et exercice", "https://diabetes.org/health-wellness/fitness/blood-glucose-and-exercise"]],
    },
    "glycemie-10-km": {
        "title": "Glycémie sur 10 km : suivre un effort court et intense",
        "description": "Glycémie pendant un 10 km : rapprocher capteur glucose, allure, fréquence cardiaque et intensité pour relire la course.",
        "keywords": "glycémie 10 km, diabète course 10k, glucose running intensif, capteur glycémie course",
        "kicker": "10 km et intensité",
        "intro": "Un 10 km concentre un effort soutenu sur une durée courte. Stress de course et intensité peuvent rendre la réponse différente d’un footing facile.",
        "medical_notice": "Une tendance observée sur une course ne permet pas d’ajuster seul un traitement. Utilisez votre protocole personnel et l’avis de vos soignants.",
        "sections": [["Distinguer course et footing", "Comparer des efforts de nature proche évite de tirer une conclusion à partir de séances très différentes."], ["Croiser allure et cardio", "Le tracé coloré par glucose replace les changements de mesure dans le niveau d’intensité réel."], ["Conserver le contexte", "Échauffement, heure du repas, stress et récupération complètent utilement la lecture du graphique."]],
        "faq": [["Pourquoi la glycémie peut-elle monter pendant un effort intense ?", "Des hormones liées à l’effort et au stress peuvent contribuer à une hausse, avec une réponse propre à chacun."], ["Le graphique sert-il d’alarme ?", "Non. Il s’agit d’une analyse sportive, pas d’un dispositif d’alerte médicale."]],
        "tracking_cta": True,
        "sources": [["American Diabetes Association — hausse du glucose pendant l’exercice", "https://diabetes.org/health-wellness/fitness/why-does-exercise-sometimes-raise-blood-sugar"]],
    },
}


# Série éditoriale consacrée aux apports horaires. Chaque page répond à une
# recherche chiffrée précise tout en rappelant qu'une cible est à tester et
# qu'elle ne constitue pas une prescription, notamment en cas de diabète.
_CARB_SOURCES = [
    ["Jeukendrup — recommandations d’apport glucidique pendant l’effort (PubMed)", "https://pubmed.ncbi.nlm.nih.gov/23765351/"],
    ["Revue 2026 — glucides et endurance, des recommandations aux apports élevés (PubMed)", "https://pubmed.ncbi.nlm.nih.gov/41759826/"],
    ["NIDDK — rôle du pancréas, de l’insuline et du glucagon", "https://www.niddk.nih.gov/-/media/Files/Diabetes/Causes_of_Diabetes_508.pdf"],
    ["American Diabetes Association — activité physique et diabète", "https://diabetesjournals.org/care/article/39/11/2065/37249/Physical-Activity-Exercise-and-Diabetes-A-Position"],
]

_CARB_DOSES = list(range(40, 121, 5))

SEO_GUIDES["glucides-par-heure"] = {
    "title": "Combien de glucides par heure en trail, running ou vélo ?",
    "description": "Comparer les apports de 40 à 120 g de glucides par heure, comprendre glucose, fructose, glycémie et tolérance digestive, puis construire son historique.",
    "keywords": "glucides par heure, nutrition trail, glucides running, glucides vélo, glycémie effort, maltodextrine fructose",
    "kicker": "Dossier nutrition d’endurance",
    "intro": "Une quantité de glucides par heure est un repère de préparation, pas une valeur universelle. La durée, l’intensité, les produits, la tolérance digestive et, le cas échéant, le traitement du diabète changent complètement la manière de l’utiliser.",
    "medical_notice": "Running Data Plan n’établit ni prescription nutritionnelle ni adaptation d’insuline. En cas de diabète, toute stratégie d’apports et de traitement pendant l’effort doit être préparée avec l’équipe soignante.",
    "sections": [
        ["De 40 à 60 g/h : une plage courante à documenter", "Ces quantités peuvent être réparties en petites prises régulières. Le résultat dépend moins d’un chiffre isolé que de la capacité à le tenir, à boire correctement et à conserver une bonne tolérance au fil des heures."],
        ["De 65 à 90 g/h : réfléchir aux glucides transportables", "Lorsque l’apport augmente, associer des sources utilisant différentes voies d’absorption, notamment glucose ou maltodextrine avec fructose, peut faciliter l’oxydation des glucides ingérés. La composition exacte du produit et sa tolérance restent à tester."],
        ["De 95 à 120 g/h : une stratégie avancée", "Ces apports élevés sont observés chez certains sportifs entraînés, mais ils ne sont ni nécessaires ni démontrés comme supérieurs pour tout le monde. Ils demandent une progression, des essais répétés et une attention particulière aux symptômes digestifs."],
        ["Glycémie, pancréas et historique personnel", "Chez une personne sans diabète, le pancréas module notamment insuline et glucagon pour contribuer à la régulation du glucose. Un capteur mesure le glucose interstitiel, avec ses limites : l’historique sert à rapprocher alimentation, relief, allure et cardio, pas à attribuer automatiquement une variation à un aliment."],
    ],
    "faq": [
        ["Quelle quantité choisir pour commencer ?", "Choisissez avec un professionnel une cible cohérente avec la durée et votre expérience, puis testez-la progressivement à l’entraînement plutôt que pour la première fois en course."],
        ["Un capteur de glycémie indique-t-il combien manger ?", "Non. Il ajoute une mesure contextuelle mais ne calcule pas à lui seul un besoin nutritionnel et ne remplace pas les consignes médicales."],
    ],
    "sources": _CARB_SOURCES,
    "carb_hub": True,
    "history_cta": True,
}

SEO_GUIDES["types-glucides-endurance"] = {
    "title": "Glucose, fructose, maltodextrine : quels glucides en endurance ?",
    "description": "Comprendre les types de glucides utilisés en trail, running et vélo : glucose, fructose, maltodextrine, saccharose, absorption et tolérance.",
    "keywords": "glucose fructose maltodextrine endurance, types glucides trail, glucides transportables, nutrition running vélo",
    "kicker": "Molécules et nutrition sportive",
    "intro": "Deux produits annonçant la même quantité de glucides peuvent employer des formules différentes. Comprendre leurs grandes familles aide à lire une étiquette et à documenter la tolérance, sans décréter qu’un mélange convient à tous.",
    "medical_notice": "Les exemples décrivent des mécanismes généraux. Allergie, maladie digestive, diabète ou traitement nécessitent un conseil adapté auprès d’un professionnel qualifié.",
    "sections": [
        ["Glucose et maltodextrine", "Le glucose est un monosaccharide directement utilisable. La maltodextrine est un assemblage de molécules issues de l’amidon, rapidement digéré en glucose. Leur goût et leur concentration peuvent différer, mais ils alimentent principalement la même voie d’absorption intestinale."],
        ["Fructose et association de transporteurs", "Le fructose emprunte une autre voie d’absorption que le glucose. Dans les apports élevés, une association glucose ou maltodextrine avec fructose peut augmenter l’utilisation de glucides exogènes. Cela ne supprime pas le risque d’inconfort digestif."],
        ["Saccharose et aliments réels", "Le saccharose fournit glucose et fructose. Fruits secs, compotes, barres ou aliments salés ajoutent texture, eau, fibres, lipides ou protéines : leur quantité totale de glucides et leur tolérance se lisent dans le contexte de la course."],
        ["Comparer avec son propre historique", "Consigne la marque, la composition, la quantité, l’eau et les symptômes. Une série d’essais comparables permet de distinguer plus proprement quantité, formule et conditions d’effort."],
    ],
    "faq": [["La maltodextrine est-elle un sucre lent ?", "Malgré un goût parfois peu sucré, elle peut être rapidement digérée en glucose ; le goût ne prédit pas la vitesse d’utilisation."], ["Faut-il toujours mélanger glucose et fructose ?", "Non. L’intérêt dépend notamment de la quantité horaire, de la durée et de la tolérance."]],
    "sources": _CARB_SOURCES,
    "history_cta": True,
}

SEO_GUIDES["variations-glycemie-pendant-effort"] = {
    "title": "Variations de glycémie pendant l’effort : comment les comprendre ?",
    "description": "Glycémie qui monte ou baisse pendant le sport : rôle de l’intensité, des glucides, du stress, du CGM et de l’historique d’entraînement.",
    "keywords": "variation glycémie effort, glycémie monte sport, glycémie baisse running, glucose trail CGM",
    "kicker": "Glycémie et contexte d’effort",
    "intro": "Une courbe qui monte ou descend pendant une activité ne raconte pas sa cause à elle seule. L’intensité, les apports, les hormones, le moment du repas et, chez une personne diabétique, l’insuline active peuvent agir simultanément.",
    "medical_notice": "Une analyse rétrospective n’est pas un système d’alerte. En présence de symptômes ou d’une valeur inattendue, appliquez les consignes de votre dispositif et de votre équipe soignante.",
    "sections": [
        ["Pourquoi une endurance modérée peut accompagner une baisse", "Le muscle augmente sa consommation de carburant pendant l’exercice. La réponse observée dépend toutefois des réserves, des apports, de l’intensité et de la régulation hormonale."],
        ["Pourquoi un effort intense peut accompagner une hausse", "Le stress de compétition et les efforts intenses peuvent stimuler des hormones favorisant la mise à disposition de glucose. Cette réponse est variable et ne permet pas de diagnostiquer une anomalie sur une seule séance."],
        ["Ce que mesure réellement un CGM", "Un capteur mesure le glucose du liquide interstitiel. Lors d’une variation rapide, la valeur peut différer temporairement du glucose sanguin ; il faut tenir compte des flèches, des symptômes et des règles propres au dispositif."],
        ["Relier la courbe au terrain", "Positionner les plages glycémiques sur l’altitude, l’allure, la VAM et le cardio aide à retrouver le contexte exact. Plusieurs activités comparables permettent ensuite de chercher une tendance reproductible."],
    ],
    "faq": [["Une hausse signifie-t-elle que j’ai trop mangé ?", "Pas nécessairement. L’intensité, le stress et d’autres facteurs peuvent intervenir ; une coïncidence n’établit pas la cause."], ["Peut-on comparer deux capteurs différents ?", "Le matériel, le site de pose et le contexte peuvent influencer la lecture. Documentez ces éléments avant de comparer."]],
    "sources": _CARB_SOURCES,
    "history_cta": True,
}

SEO_GUIDES["insuline-pancreas-et-sport"] = {
    "title": "Insuline, pancréas et sport : comprendre la régulation du glucose",
    "description": "Comprendre simplement le rôle du pancréas, de l’insuline, du glucagon et du glucose pendant le sport, avec ou sans diabète.",
    "keywords": "insuline sport pancréas, glucagon exercice, glycémie endurance, fonctionnement pancréas glucose",
    "kicker": "Physiologie du glucose",
    "intro": "Le pancréas participe en permanence à l’équilibre énergétique. Pendant l’effort, muscles, foie, hormones et alimentation interagissent : ce système explique pourquoi une quantité de glucides ne produit pas une courbe identique chez deux personnes.",
    "medical_notice": "Cette présentation simplifiée ne permet aucune adaptation de traitement. Toute décision concernant l’insuline doit suivre un protocole établi avec l’équipe de diabétologie.",
    "sections": [
        ["Les cellules bêta et l’insuline", "Lorsque le glucose augmente, les cellules bêta des îlots pancréatiques sécrètent de l’insuline. Celle-ci facilite notamment l’entrée du glucose dans certains tissus et favorise son stockage sous forme de glycogène."],
        ["Les cellules alpha et le glucagon", "Lorsque la disponibilité du glucose baisse, le glucagon contribue à signaler au foie de libérer du glucose. Insuline et glucagon participent ainsi à une régulation dynamique, et non à un simple interrupteur."],
        ["Ce que l’exercice change", "Les muscles actifs utilisent davantage de carburant et leur captation du glucose peut augmenter. En parallèle, l’intensité et le stress modifient la réponse hormonale et peuvent favoriser une hausse transitoire chez certaines personnes."],
        ["Pourquoi le diabète impose une stratégie personnelle", "Dans le diabète de type 1, l’insuline doit être apportée de l’extérieur. Dans le diabète de type 2, sa production ou son action peut être insuffisante. L’effort, les glucides et les médicaments doivent donc être préparés avec des professionnels connaissant la personne."],
    ],
    "faq": [["Le pancréas libère-t-il seulement de l’insuline ?", "Non. Il produit notamment le glucagon, qui joue un rôle complémentaire dans la régulation du glucose."], ["Le sport fait-il toujours baisser la glycémie ?", "Non. Le type d’effort, son intensité et le contexte peuvent produire des réponses différentes."]],
    "sources": _CARB_SOURCES,
    "history_cta": True,
}


def _carb_dose_guide(grams: int) -> dict:
    per_15 = grams / 4
    per_20 = grams / 3
    per_30 = grams / 2
    if grams <= 60:
        level = "repère modéré"
        context = ("Cette cible se situe dans une plage fréquemment utilisée pendant les efforts d’endurance. "
                   "Elle peut servir de point de travail si la durée, l’intensité et l’alimentation préalable la justifient.")
        molecule = ("À ce niveau, une source apportant du glucose, du saccharose ou de la maltodextrine peut suffire selon le produit. "
                    "La présence de fructose n’est pas automatiquement synonyme de meilleure tolérance : lis l’étiquette et teste la formule complète.")
    elif grams <= 90:
        level = "apport soutenu"
        context = ("Cette cible appartient à une plage soutenue, surtout pertinente lors d’efforts longs. "
                   "Plus la quantité augmente, plus la répartition, la boisson et l’entraînement digestif deviennent déterminants.")
        molecule = ("Les recommandations sur les apports élevés évoquent des glucides utilisant plusieurs transporteurs intestinaux, typiquement glucose ou maltodextrine associés au fructose. "
                    "Le ratio varie selon les produits : le total réellement ingéré et la tolérance comptent davantage qu’un argument marketing.")
    else:
        level = "stratégie très élevée"
        context = ("Cette cible dépasse la limite historique de 90 g/h et relève d’une stratégie avancée. "
                   "Des sportifs entraînés expérimentent jusqu’à 120 g/h, mais la littérature ne démontre pas un bénéfice universel et les données restent en évolution.")
        molecule = ("À ce niveau, une seule source de glucose est généralement une mauvaise simplification : les travaux portent plutôt sur des mélanges glucose ou maltodextrine et fructose. "
                    "Même avec un mélange, absorber davantage ne garantit ni oxydation complète ni confort digestif.")

    return {
        "title": f"{grams} g de glucides par heure : stratégie nutrition en endurance",
        "description": f"Comment répartir {grams} g de glucides par heure en trail, running ou vélo : prises, molécules, glycémie, digestion et suivi personnel.",
        "keywords": f"{grams} g glucides par heure, {grams}gr glucides heure, nutrition trail, alimentation endurance, glycémie sport, glucose fructose maltodextrine",
        "kicker": f"{grams} g/h · {level}",
        "intro": f"Viser {grams} g de glucides par heure revient à organiser un débit moyen, pas à avaler {grams} g en une fois. Une répartition régulière donne des repères plus faciles à tester, mais cette cible doit rester cohérente avec ton effort et ta tolérance.",
        "medical_notice": "Cette page fournit des repères généraux et ne prescrit pas une quantité. En cas de diabète ou de traitement influençant la glycémie, ne modifiez ni apports ni insuline à partir de cette page : préparez un protocole personnel avec l’équipe soignante.",
        "sections": [
            [f"Que représente exactement {grams} g/h ?", f"Sur une heure, cela correspond par exemple à environ {per_15:g} g toutes les 15 minutes, {per_20:.1f} g toutes les 20 minutes ou {per_30:g} g toutes les 30 minutes. Ces divisions sont des équivalences mathématiques : gels, boissons et aliments doivent être additionnés d’après leur étiquette."],
            [f"Dans quel contexte envisager {grams} g/h ?", context],
            ["Glucose, maltodextrine, fructose et saccharose", molecule],
            ["Glycémie : observer une variation sans inventer une cause", "Un apport glucidique, l’intensité, le stress, la température, l’insuline active et le fonctionnement hormonal peuvent coïncider avec une variation. Le CGM mesure le glucose interstitiel et peut présenter un décalage lors des changements rapides. Il faut donc rapprocher la courbe du moment de prise, du cardio, de l’allure et du relief, puis comparer plusieurs séances similaires."],
            ["Insuline, glucagon et fonctionnement du pancréas", "Le pancréas endocrine participe à l’équilibre du glucose : les cellules bêta sécrètent l’insuline, qui favorise notamment l’utilisation et le stockage du glucose, tandis que le glucagon contribue à mobiliser du glucose lorsque sa disponibilité baisse. Pendant l’effort, cette régulation interagit avec les hormones du stress et la consommation énergétique. En cas de diabète, ce fonctionnement est modifié et exige des consignes individualisées."],
            ["Transformer un essai en historique utile", f"Enregistre la quantité réellement prise, les horaires, le produit, l’eau consommée et les symptômes. Avec Running Data Plan, les activités, le relief, l’allure, le cardio et les données CGM compatibles peuvent être conservés au même endroit. Plusieurs essais à {grams} g/h permettent de comparer, sans prétendre prouver qu’une variation vient d’un seul facteur."],
        ],
        "faq": [
            [f"{grams} g/h convient-il à tout le monde ?", "Non. La cible dépend de l’effort, de l’expérience, de la tolérance digestive et de la situation médicale. Elle doit être testée progressivement dans un cadre adapté."],
            ["Faut-il compter les glucides de la boisson ?", "Oui. Le total horaire comprend boissons, gels, barres et aliments. Utilisez les grammes de glucides indiqués sur les étiquettes, pas seulement le poids du produit."],
            ["La glycémie suffit-elle pour valider la stratégie ?", "Non. Ajoutez au minimum sensations, tolérance digestive, allure, intensité et contexte. Pour toute décision médicale, suivez les consignes de votre équipe soignante."],
        ],
        "sources": _CARB_SOURCES,
        "carb_dose": {
            "grams": grams,
            "per_15": f"{per_15:g}",
            "per_20": f"{per_20:.1f}",
            "per_30": f"{per_30:g}",
        },
        "history_cta": True,
        "index_card": False,
    }


for _grams in _CARB_DOSES:
    SEO_GUIDES[f"{_grams}-g-glucides-par-heure"] = _carb_dose_guide(_grams)
