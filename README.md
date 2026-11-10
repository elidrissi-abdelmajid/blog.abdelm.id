Résumé pour CV (version française)
Refonte du job Client360 – Migration vers une architecture Data Product (Datalake Bronze / Silver / Gold) Équipe Data Engineering – Spark / Scala, Hadoop, HBase, Airflow
•
Participation à la migration du job monolithique Client360Job, qui s'exécutait plus de 3 h, deux fois par jour, vers une architecture Data Product en couches Bronze → Silver → Gold.
•
Développement d'environ 45 transformations Silver par domaine métier (Carte, CRM, PNB, Titres, Trade Finance, etc.) et d'environ 25 agrégations Gold « Customer 360 », avec un code mutualisé et des data contracts (schémas typés).
•
Découpage du traitement en plannings indépendants par domaine et orchestration de la couche Gold avec Apache Airflow. Les domaines sont désormais traités en parallèle, ce qui réduit le temps de traitement et la durée d'exécution globale.
•
Optimisation du stockage : les données sont créées dans Oracle, ce qui allège la charge sur HDFS.
•
Application de design patterns (Factory, Strategy, Template Method, DI) pour un code modulaire, maintenable et plus facile à faire évoluer.
Stack : Spark, Scala, Hive, HBase, HDFS, Oracle, Airflow, architecture Medallion.
