package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App {
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains", (queryRectangle: String, pointString: String) => (calculate_ST_Contains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from point where ST_Contains('" + arg2 + "',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def calculate_ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    val coordinates_rectangle = queryRectangle.split(",")
    val x1 = coordinates_rectangle(0).toDouble
    val y1 = coordinates_rectangle(1).toDouble
    val x2 = coordinates_rectangle(2).toDouble
    val y2 = coordinates_rectangle(3).toDouble

    val coordinate_point = pointString.split(",")
    val x = coordinate_point(0).toDouble
    val y = coordinate_point(1).toDouble

    if ((x >= Math.min(x1, x2) && x <= Math.max(x1, x2)) && (y >= Math.min(y1, y2) && y <= Math.max(y1, y2))) {
      true
    }
    else {
      false
    }

  }

  def calculate_ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean = {

    val coordinate_pointString1 = pointString1.split(",")
    val x1 = coordinate_pointString1(0).toDouble
    val y1 = coordinate_pointString1(1).toDouble

    val coordinate_pointString2 = pointString2.split(",")
    val x2 = coordinate_pointString2(0).toDouble
    val y2 = coordinate_pointString2(1).toDouble

    val calculated_distance = Math.sqrt(Math.pow(Math.abs(x1 - x2), 2) + Math.pow(Math.abs(y1 - y2), 2))
    if (calculated_distance <= distance) {
      true
    }
    else {
      false
    }

  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains", (queryRectangle: String, pointString: String) => (calculate_ST_Contains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within", (pointString1: String, pointString2: String, distance: Double) => (calculate_ST_Within(pointString1: String, pointString2: String, distance: Double)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'" + arg2 + "'," + arg3 + ")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within", (pointString1: String, pointString2: String, distance: Double) => (calculate_ST_Within(pointString1: String, pointString2: String, distance: Double)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, " + arg3 + ")")
    resultDf.show()

    return resultDf.count()
  }
}
