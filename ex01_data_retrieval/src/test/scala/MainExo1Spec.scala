import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import java.io.File
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI

class MainExo1Spec extends AnyFunSuite with BeforeAndAfterAll {

  // Configuration Spark identique à MainExo1 pour l'accès S3A
  val spark: SparkSession = SparkSession.builder()
    .appName("Test_Exo1_Full_Validation")
    .master("local[1]")
    .config("fs.s3a.endpoint", "http://localhost:9000")
    .config("fs.s3a.access.key", "minio")
    .config("fs.s3a.secret.key", "minio123")
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()


  // Chemins locaux définis dans ton sujet
  val pathJuin = "../data/raw/yellow_tripdata_2024-06.parquet"
  val pathDec = "../data/raw/yellow_tripdata_2024-12.parquet"

  test("1. Validation de la présence des fichiers sources locaux") {
    // Vérifie si les fichiers de Juin et Décembre existent
    assert(new File(pathJuin).exists(), s"ERREUR : Fichier de Juin introuvable dans $pathJuin")
    assert(new File(pathDec).exists(), s"ERREUR : Fichier de Décembre introuvable dans $pathDec")
  }

  test("2. Validation de la structure (Schema) des fichiers Parquet") {
    val dfJuin = spark.read.parquet(pathJuin).limit(1)
    val dfDec = spark.read.parquet(pathDec).limit(1)

    // Colonnes critiques pour la target du ML et l'ingestion Gold
    val mandatoryCols = Seq("tpep_pickup_datetime", "tpep_dropoff_datetime", "total_amount", "trip_distance", "PULocationID", "DOLocationID")

    mandatoryCols.foreach { col =>
      assert(dfJuin.columns.contains(col), s"Colonne '$col' manquante dans le fichier de Juin")
      assert(dfDec.columns.contains(col), s"Colonne '$col' manquante dans le fichier de Décembre")
    }
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }
}
