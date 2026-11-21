"""TP Spark SQL Bike Sharing — version PySpark (6 exercices).

Usage:
  python main.py data/bike_sharing.csv
  spark-submit main.py data/bike_sharing.csv

Passez le vrai CSV du TP Java pour retrouver les mêmes résultats.
"""
from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType


SCHEMA = StructType([
    StructField("rental_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("start_station", StringType(), True),
    StructField("end_station", StringType(), True),
    StructField("duration_minutes", IntegerType(), True),
    StructField("price", DoubleType(), True),
])


def section(title):
    print("\n" + "=" * 18 + " " + title + " " + "=" * 18)


def main():
    csv_arg = sys.argv[1] if len(sys.argv) > 1 else "data/bike_sharing.csv"
    csv_path = csv_arg if "://" in csv_arg else str(Path(csv_arg).expanduser().resolve())

    spark = SparkSession.builder.appName("TP-Spark-SQL-Bike-Sharing-Python").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = (spark.read.option("header", True).option("mode", "FAILFAST")
              .schema(SCHEMA).csv(csv_path))
        if not df.take(1):
            raise ValueError(f"Fichier CSV vide ou introuvable: {csv_path}")

        section("EXERCICE 1 : EXPLORATION")
        print("1.1 Schema du DataFrame")
        df.printSchema()
        print("1.2 Les cinq premieres lignes")
        df.show(5, truncate=False)
        print("1.3 Nombre total de locations")
        print("Total rentals:", df.count())

        section("EXERCICE 2 : VUE TEMPORAIRE")
        df.createOrReplaceTempView("bike_rentals_view")
        print("Vue temporaire bike_rentals_view creee")

        section("EXERCICE 3 : REQUETES DE BASE")
        print("3.1 Locations de plus de 30 minutes")
        spark.sql("""SELECT rental_id, user_id, duration_minutes, start_station, price
                     FROM bike_rentals_view
                     WHERE duration_minutes > 30
                     ORDER BY duration_minutes DESC, rental_id""").show(100, truncate=False)
        print("3.2 Locations au depart de Station A")
        spark.sql("""SELECT rental_id, user_id, start_station, duration_minutes
                     FROM bike_rentals_view WHERE start_station = 'Station A'
                     ORDER BY rental_id""").show(100, truncate=False)
        print("3.3 Recette totale")
        spark.sql("""SELECT ROUND(SUM(price), 2) AS total_revenue
                     FROM bike_rentals_view""").show(truncate=False)

        section("EXERCICE 4 : AGREGATIONS")
        print("4.1 Nombre de locations par station")
        spark.sql("""SELECT start_station, COUNT(*) AS rental_count
                     FROM bike_rentals_view GROUP BY start_station
                     ORDER BY rental_count DESC, start_station""").show(truncate=False)
        print("4.2 Duree moyenne par station")
        spark.sql("""SELECT start_station, ROUND(AVG(duration_minutes), 2) AS avg_duration_minutes
                     FROM bike_rentals_view GROUP BY start_station
                     ORDER BY start_station""").show(truncate=False)
        print("4.3 Station la plus populaire")
        spark.sql("""SELECT start_station, COUNT(*) AS rental_count
                     FROM bike_rentals_view GROUP BY start_station
                     ORDER BY rental_count DESC, start_station LIMIT 1""").show(truncate=False)

        section("EXERCICE 5 : ANALYSE TEMPORELLE")
        print("5.1 Extraction de l'heure de depart")
        df.select("rental_id", "start_time", F.hour("start_time").alias("start_hour")) \
          .orderBy("rental_id").show(60, truncate=False)
        print("5.2 Locations par heure")
        spark.sql("""SELECT HOUR(start_time) AS start_hour, COUNT(*) AS rental_count
                     FROM bike_rentals_view GROUP BY HOUR(start_time)
                     ORDER BY rental_count DESC, start_hour""").show(100, truncate=False)
        print("5.3 Station la plus populaire le matin (07h-12h, 12h exclue)")
        spark.sql("""SELECT start_station, COUNT(*) AS morning_rentals
                     FROM bike_rentals_view
                     WHERE HOUR(start_time) BETWEEN 7 AND 12                     GROUP BY start_station
                     ORDER BY morning_rentals DESC, start_station LIMIT 1""").show(truncate=False)

        section("EXERCICE 6 : UTILISATEURS")
        print("6.1 Age moyen")
        spark.sql("""SELECT ROUND(AVG(age), 2) AS average_age
                     FROM bike_rentals_view""").show(truncate=False)
        print("6.2 Locations par genre")
        spark.sql("""SELECT gender, COUNT(*) AS unique_users_rentals
                     FROM bike_rentals_view GROUP BY gender ORDER BY gender""").show(truncate=False)
        print("6.3 Locations par tranche d'age")
        age_groups = df.withColumn(
            "age_group",
            F.when(F.col("age").between(18, 30), "18-30")
             .when(F.col("age").between(31, 40), "31-40")
             .when(F.col("age").between(41, 50), "41-50")
             .when(F.col("age") >= 51, "51+")
             .otherwise("Hors tranche"),
        )
        (age_groups.groupBy("age_group").count()
         .withColumnRenamed("count", "rental_count")
         .orderBy(F.when(F.col("age_group") == "18-30", 1)
                  .when(F.col("age_group") == "31-40", 2)
                  .when(F.col("age_group") == "41-50", 3)
                  .when(F.col("age_group") == "51+", 4).otherwise(5))
         .show(truncate=False))
        section("TP SPARK SQL PYSPARK TERMINE AVEC SUCCES")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
