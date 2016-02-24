/**
  * Created by Vipul on 25/2/16.
  * Example usage of Dataframes API,udf and join
  */

package me.vipulrai

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

object UdfTest {

  def  main (args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("my_udfs")
    val sc =  new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val baseDf = sqlContext.read.parquet("/home/affine/Spark_Test_Dir/DataSets/parquetfiles/*")

    val airCode = sc.textFile("/home/affine/Downloads/airport_code_all.csv")

    val schemaString = "port_code,port_name"

    val schema =
      StructType(
        schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val rowRDD = airCode.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    // Apply the schema to the RDD.
    val airPortDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // Find the first element in the WrappedArray
    val findCode = udf((code : scala.collection.mutable.WrappedArray[String]) =>{
      val ret = if (code == null) "empty" else if(code.length > 0) code(0) else "empty"
      ret
    })

      //udf to replace null if any
    val replaceNull  = udf((col_val :String) =>  if(col_val == null) "empty" else col_val)

    val select_attr_Df = baseDf.filter("action = 'SEARCH' and domain = 'AIR'")
      .select( $"entity.flightSearch.searchParameters.departureAirportCityState" as "flight_search_origin",
        $"entity.flightSearch.searchParameters.arrivalAirportCityState" as "flight _search_destination",
        $"entity.flightSearch.searchParameters.travelClass" as "flght_search_travel_class",
        $"entity.flightSearch.searchParameters.departureDate" as "flight_search_departure_date ",
        $"entity.flightSearch.searchParameters.arrivalAirport" as "flight_search_origin_airportcode" ,
        $"entity.flightSearch.searchParameters.departureAirport" as "flight_search_destination_airportcode")
       .withColumn("flight_search_origin_airportcode_not_null" , replaceNull($"flight_search_origin_airportcode"))

    val codeMappedDf = select_attr_Df.join(airPortDataFrame , $"flight_search_origin_airportcode" === $"port_code" , "leftouter")


    codeMappedDf.show()

  }

}
