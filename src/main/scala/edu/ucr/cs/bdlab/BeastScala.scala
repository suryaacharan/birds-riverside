package edu.ucr.cs.bdlab

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.{CRSServer, SparkSQLRegistration}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object BeastScala {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Bird Count")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    // Start the CRSServer and store the information in SparkConf
    CRSServer.startServer(conf)
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    try {
      import edu.ucr.cs.bdlab.beast._
      val birdDataPoints : SpatialRDD = sparkContext.geojsonFile("eBird.geojson")
      val riversideParkData : SpatialRDD = sparkContext.shapefile("Riverside_Parks.zip")

      val birdsInParkRDD: RDD[(IFeature, IFeature)] = riversideParkData.spatialJoin(birdDataPoints)

      val birdsInParkDF: DataFrame =birdsInParkRDD.map({ case (park, bird) => Feature.append(park, Try(bird.getAs[String]("OBSERVATION COUNT").toInt).getOrElse(0), "Bird",IntegerType)}).toDataFrame(sparkSession)
      val result : DataFrame= birdsInParkDF.groupBy("PARK_NAME","g").sum("Bird").as("Bird Observations")
      result.toSpatialRDD.coalesce(1).saveAsGeoJSON("birdsInPark")
    } finally {
      sparkSession.stop()
      CRSServer.stopServer()
    }
  }
}