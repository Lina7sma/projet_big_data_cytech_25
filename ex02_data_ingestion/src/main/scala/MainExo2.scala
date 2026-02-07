/*
 * Ce programme lit les données de taxi stockées sur MinIO,
 * nettoie et transforme les trajets de juin et décembre,
 * Charge les données finales dans PostgreSQL
 * pour remplir les tables de données (fact_trips et dim_zone).
 */

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.io.File
import java.sql.DriverManager
import java.util.Properties

object MainExo2 {
  def main(args: Array[String]): Unit = {

    // --- DÉTECTION DE L'ENVIRONNEMENT ---
    val isDocker = new File("/.dockerenv").exists()
    val minioHost = if (isDocker) "minio" else "localhost"
    val pgHost = if (isDocker) "postgres" else "localhost"

    // 1. Initialisation de la SparkSession
    val spark = SparkSession.builder()
      .appName("NYC_Taxi_Data_Warehouse")
      .master("local[*]")
      .getOrCreate()

    // 2. Configuration MinIO (S3A)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s"http://$minioHost:9000")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "minio")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "minio123")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    // 3. Paramètres JDBC
    val jdbcUrl = s"jdbc:postgresql://$pgHost:5432/postgres"
    val dbProps = new Properties()
    dbProps.setProperty("user", "postgres")
    dbProps.setProperty("password", "postgres")
    dbProps.setProperty("driver", "org.postgresql.Driver")

    // --- ÉTAPE A : VÉRIFICATION STRICTE DES TABLES DU PROF ---
    // On vérifie que les tables créées via SQL existent. Si non -> Erreur et arrêt.
    println(">>> Vérification des tables officielles (SQL)...")
    val conn = DriverManager.getConnection(jdbcUrl, "postgres", "postgres")
    val stmt = conn.createStatement()

    // Liste des tables qui DOIVENT exister via creation.sql
    val mandatoryTables = Seq("fact_trips", "dim_zone")

    mandatoryTables.foreach { table =>
      try {
        // TRUNCATE vide la table mais garde la structure SQL (colonnes, types, SERIAL)
        // RESTART IDENTITY remet le compteur du trip_id à 1
        stmt.execute(s"TRUNCATE TABLE $table RESTART IDENTITY CASCADE")
        println(s"  [OK] Table '$table' trouvée et vidée.")
      } catch {
        case e: Exception =>
          println(s"\n[ERREUR CRITIQUE] : La table '$table' est manquante !")
          println("Veuillez exécuter le script 'creation.sql' avant de lancer le code Scala.")
          println(s"Détail technique : ${e.getMessage}")
          conn.close()
          spark.stop()
          sys.exit(1) // Arrêt immédiat du programme
      }
    }
    conn.close()

    // --- ÉTAPE B : IMPORTATION DES RÉFÉRENTIELS (dim_zone) ---
    println(">>> Chargement de la table de référence : dim_zone...")
    val zonesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("../data/raw/taxi_zone_lookup.csv")

    val zonesToDb = zonesDF.select(
      col("LocationID").as("location_id"),
      col("Borough").as("borough"),
      col("Zone").as("zone"),
      col("service_zone").as("service_zone")
    )

    // Mode Append car la table existe déjà (merci le TRUNCATE plus haut)
    zonesToDb.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dim_zone", dbProps)

    // --- ÉTAPE C : FONCTION DE TRAITEMENT DES MOIS ---
    def processMonth(monthPath: String, monthLabel: String, rawTableName: String): Unit = {
      println(s"\n>>> Traitement du mois : $monthLabel")

      // Lecture du Parquet sur MinIO
      val df = spark.read.parquet(monthPath)

      // 1. Sauvegarde RAW (Tes tables perso : Scala peut les créer/écraser)
      println(s"  - Création du backup perso : $rawTableName")
      df.write.mode(SaveMode.Overwrite).jdbc(jdbcUrl, rawTableName, dbProps)

      // 2. Nettoyage : Filtre sur les montants/distance ET validation stricte du mois
      // On utilise le paramètre monthLabel pour savoir si on cherche le mois 6 ou 12
      val targetMonth = if (monthLabel == "JUIN") 6 else 12

      val cleaned = df.filter(
        col("total_amount") > 0 &&
          col("trip_distance") > 0 &&
          (month(col("tpep_pickup_datetime")) === targetMonth || month(col("tpep_dropoff_datetime")) === targetMonth)
      )

      // 3. Transformation pour fact_trips (Ordre strict correspondant au SQL)
      val toWarehouse = cleaned.select(

        when(month(col("tpep_pickup_datetime")) === (targetMonth - 1), lit(targetMonth))
          .otherwise(month(col("tpep_pickup_datetime")))
          .as("trip_month"),

        col("tpep_pickup_datetime").as("pickup_datetime"),
        col("tpep_dropoff_datetime").as("dropoff_datetime"),
        col("total_amount"),
        col("passenger_count"),
        col("trip_distance"),
        col("PULocationID").as("pickup_location_id"),
        col("DOLocationID").as("dropoff_location_id"),
        col("VendorID").as("vendor_id"),
        col("fare_amount"),
        col("tip_amount"),
        coalesce(col("RatecodeID"), lit(99)).as("rate_code_id"),
        coalesce(col("payment_type"), lit(5)).as("payment_type_id"),
        col("tolls_amount"),
        col("extra"),
        col("mta_tax"),
        col("improvement_surcharge"),
        col("congestion_surcharge"),
        coalesce(col("Airport_fee"), lit(0)).as("airport_fee")
      )

      // 4. Injection dans fact_trips
      // On utilise APPEND pour ne pas supprimer la colonne trip_id (SERIAL) du SQL
      println(s"  - Injection des données nettoyées dans 'fact_trips'...")
      toWarehouse.write.mode(SaveMode.Append).jdbc(jdbcUrl, "fact_trips", dbProps)
    }

    // --- ÉTAPE D : EXÉCUTION ---
    processMonth("s3a://raw-data/nyc_raw/month=06", "JUIN", "trips_june_raw")
    processMonth("s3a://raw-data/nyc_raw/month=12", "DÉCEMBRE", "trips_dec_raw")

    println("\n>>> Fin du traitement avec succès.")
    spark.stop()
  }
}