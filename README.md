# TP Spark SQL — Bike Sharing (PySpark)

Conversion des 6 exercices Java Spark SQL en Python/PySpark, version **Spark 4.0.1**. Projet destiné à être exécuté d'abord en local avec le **même fichier `bike_sharing.csv`** que le TP Java, puis sur Spark Standalone avec un chemin HDFS si demandé.

## Fichiers

- `main.py` : les exercices 1 à 6 (exploration, vue temporaire, filtres, agrégations, temps, utilisateurs).
- `requirements.txt` : version de PySpark utilisée.
- `data/` : placer ici le **véritable** `bike_sharing.csv` du TP Java. Il n'est pas fourni, pour éviter d'inventer des données ou des résultats.
- `screenshots/` : captures de résultats pour le compte rendu.

## Installation sous Windows PowerShell

Installer Python 3.11 ou 3.12 et Java 17 (configurer `JAVA_HOME`). Depuis la racine du projet :

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python main.py .\data\bike_sharing.csv
```

Si PowerShell empêche l'activation du venv, utiliser directement ` .\.venv\Scripts\python.exe -m pip install -r requirements.txt ` et ` .\.venv\Scripts\python.exe main.py .\data\bike_sharing.csv `.

> Éviter d'utiliser JDK 24 ; privilégier Java 17 qui est déjà validé sur le cluster Oracle. Le script n'impose pas `local[*]` : hors `spark-submit`, PySpark utilise la configuration Spark disponible.

## Colonnes attendues dans le CSV

`rental_id,user_id,age,gender,start_time,end_time,start_station,end_station,duration_minutes,price`

Le fichier est lu avec un schéma explicite, donc `start_time` et `end_time` doivent être des timestamps acceptés par Spark (ex. `2025-10-02 08:07:00`).

## Résultats de référence de l'ancien TP Java

Sur les 60 locations du vrai jeu de données, les captures précédentes indiquaient : 60 locations, recette totale 469.08, station de départ la plus populaire Station C (13), station la plus populaire le matin Station D (8), âge moyen 37.5, locations F = 31, M = 29. Ces valeurs constituent des **résultats à comparer** une fois le vrai CSV récupéré, pas des résultats que nous avons exécutés avec le code Python.

## Exécution sur Oracle Cloud (après validation locale)

1. Copier `main.py` sur le Master. Le CSV peut être placé dans HDFS.
2. Utiliser le chemin absolu `hdfs://10.0.0.194:9000/user/ubuntu/.../bike_sharing.csv` en argument.
3. Exécuter depuis le Master :

```bash
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
/opt/spark/bin/spark-submit \
  --master spark://10.0.0.194:7077 \
  --deploy-mode client \
  --conf spark.driver.host=10.0.0.194 \
  --conf spark.driver.bindAddress=10.0.0.194 \
  --conf spark.driver.port=39000 \
  --conf spark.driver.blockManager.port=39001 \
  --conf spark.port.maxRetries=0 \
  --total-executor-cores 1 \
  --executor-memory 1g \
  /home/ubuntu/main.py \
  hdfs://10.0.0.194:9000/user/ubuntu/bike_sharing.csv
```

Le chemin HDFS est illustratif : déposer le vrai fichier à cet endroit ou adapter l'argument. Le code Python s'exécute via le driver sur Master et les tâches Spark sur Worker 1.

## Captures à prendre

1. Installation PySpark et Java 17.
2. Schéma, cinq premières lignes et total de 60 locations.
3. Requêtes de base et total 469.08.
4. Agrégations et station C.
5. Analyse horaire et station D du matin.
6. Âges et genre ; message de fin.
7. Sur cluster (optionnel) : application Spark visible et tâches sur Worker 1.

## Note

Ce TP est **Spark SQL batch** en PySpark, et non le TP Structured Streaming (TCP/CSV) réalisé séparément.
