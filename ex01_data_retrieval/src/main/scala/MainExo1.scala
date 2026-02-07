/*
 * Ce programme lit les fichiers de Juin et Décembre 2024,
 * configure une connexion sécurisée vers MinIO,
 * et transfère la totalité des données vers le bucket "raw-data".
 */

import org.apache.spark.sql.SparkSession
import io.minio.MinioClient
import io.minio.BucketExistsArgs
import io.minio.MakeBucketArgs

import java.io.File

object MainExo1 {
  def main(args: Array[String]): Unit = {

    val isDocker = new File("/.dockerenv").exists()
    val minioHost = if (isDocker) "minio" else "localhost"

    println(">>> Vérification du Bucket sur MinIO...")

    val minioClient = MinioClient.builder()
      .endpoint(s"http://$minioHost:9000") // Utilise la variable ici !
      .credentials("minio", "minio123")
      .build()

    val bucketName = "raw-data"

    // Si le bucket n'existe pas, on le crée
    if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
      println(s">>> Le bucket '$bucketName' est absent. Création en cours...")
      minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build())
      println(s">>> Bucket '$bucketName' créé avec succès.")
    } else {
      println(s">>> Bucket '$bucketName' déjà présent.")
    }

    // --- INITIALISATION SPARK ---
    val spark = SparkSession.builder()
      .appName("Ex01_Data_Retrieval_Automatic")
      .master("local[*]")
      .getOrCreate()

    // Configuration Hadoop pour S3A
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s"http://$minioHost:9000")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "minio")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "minio123")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    // --- CHARGEMENT ET TRANSFERT ---
    println(">>> Transfert JUIN 2024...")
    val juneData = spark.read.parquet("../data/raw/yellow_tripdata_2024-06.parquet")
    juneData.write.mode("overwrite").parquet(s"s3a://$bucketName/nyc_raw/month=06")

    println(">>> Transfert DÉCEMBRE 2024...")
    val decData = spark.read.parquet("../data/raw/yellow_tripdata_2024-12.parquet")
    decData.write.mode("overwrite").parquet(s"s3a://$bucketName/nyc_raw/month=12")

    println(">>> Fin du transfert avec succès.")
    spark.stop()
  }
}