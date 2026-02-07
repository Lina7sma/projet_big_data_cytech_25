import org.apache.spark.sql.{SparkSession, DataFrame}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import java.sql.DriverManager
import java.util.Properties
import java.io.File

class MainExo2Spec extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _
  val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
  val dbProps = new Properties()
  dbProps.setProperty("user", "postgres")
  dbProps.setProperty("password", "postgres")
  dbProps.setProperty("driver", "org.postgresql.Driver")

  override def beforeAll(): Unit = {
    // Initialisation de SparkSession
    spark = SparkSession.builder()
      .appName("Test_Exo2")
      .master("local[*]")
      .getOrCreate()

    // Configuration MinIO (S3A)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://localhost:9000")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "minio")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "minio123")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    // Création des tables de test dans PostgreSQL
    val conn = DriverManager.getConnection(jdbcUrl, "postgres", "postgres")
    val stmt = conn.createStatement()
    stmt.execute(
      """
        |CREATE TABLE IF NOT EXISTS dim_zone (
        |  location_id INT,
        |  borough VARCHAR(255),
        |  zone VARCHAR(255),
        |  service_zone VARCHAR(255)
        |);
                """.stripMargin)
    stmt.execute(
      """
        |CREATE TABLE IF NOT EXISTS fact_trips (
        |  trip_id SERIAL PRIMARY KEY,
        |  trip_month INT,
        |  pickup_datetime TIMESTAMP,
        |  dropoff_datetime TIMESTAMP,
        |  total_amount DOUBLE PRECISION,
        |  passenger_count INT,
        |  trip_distance DOUBLE PRECISION,
        |  pickup_location_id INT,
        |  dropoff_location_id INT,
        |  vendor_id INT,
        |  fare_amount DOUBLE PRECISION,
        |  tip_amount DOUBLE PRECISION,
        |  rate_code_id INT,
        |  payment_type_id INT,
        |  tolls_amount DOUBLE PRECISION,
        |  extra DOUBLE PRECISION,
        |  mta_tax DOUBLE PRECISION,
        |  improvement_surcharge DOUBLE PRECISION,
        |  congestion_surcharge DOUBLE PRECISION,
        |  airport_fee DOUBLE PRECISION
        |);
                """.stripMargin)
    conn.close()
  }

  test("1. Vérification des tables PostgreSQL") {
    val conn = DriverManager.getConnection(jdbcUrl, "postgres", "postgres")
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    val tables = Iterator.continually((rs, rs.next())).takeWhile(_._2).map(_._1.getString("table_name")).toSet
    conn.close()

    assert(tables.contains("dim_zone"), "La table 'dim_zone' est manquante.")
    assert(tables.contains("fact_trips"), "La table 'fact_trips' est manquante.")
  }

  test("2. Chargement et validation des données dim_zone") {
    val zonesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("../data/raw/taxi_zone_lookup.csv")

    assert(zonesDF.columns.contains("LocationID"), "Colonne 'LocationID' manquante.")
    assert(zonesDF.count() > 0, "Le fichier dim_zone est vide.")

    zonesDF.write.mode("overwrite").jdbc(jdbcUrl, "dim_zone", dbProps)

    val loadedDF = spark.read.jdbc(jdbcUrl, "dim_zone", dbProps)
    assert(loadedDF.count() == zonesDF.count(), "Les données dim_zone n'ont pas été correctement insérées.")
  }

  test("3. Traitement des données mensuelles") {
    val monthPath = "s3a://raw-data/nyc_raw/month=06"
    val rawDF = spark.read.parquet(monthPath)

    assert(rawDF.columns.contains("tpep_pickup_datetime"), "Colonne 'tpep_pickup_datetime' manquante.")
    assert(rawDF.count() > 0, "Le fichier Parquet est vide.")

    val cleanedDF = rawDF.filter(
      rawDF("total_amount") > 0 &&
        rawDF("trip_distance") > 0
    )

    assert(cleanedDF.count() > 0, "Aucune donnée valide après nettoyage.")

    cleanedDF.write.mode("overwrite").jdbc(jdbcUrl, "fact_trips", dbProps)

    val loadedDF = spark.read.jdbc(jdbcUrl, "fact_trips", dbProps)
    assert(loadedDF.count() == cleanedDF.count(), "Les données fact_trips n'ont pas été correctement insérées.")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    val conn = DriverManager.getConnection(jdbcUrl, "postgres", "postgres")
    val stmt = conn.createStatement()
    stmt.execute("DROP TABLE IF EXISTS dim_zone")
    stmt.execute("DROP TABLE IF EXISTS fact_trips")
    conn.close()
  }
}


