package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App {
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains", (queryRectangle: String, pointString: String) => ({
      if (queryRectangle != null && pointString != null) {

        val queryRectanglearr = queryRectangle.split(",")
        val pointStringarr = pointString.split(",")
        if (queryRectanglearr.length == 4 || pointStringarr.length == 2) {
          val maxX = math.max(queryRectanglearr(0).toDouble, queryRectanglearr(2).toDouble)
          val minX = math.min(queryRectanglearr(0).toDouble, queryRectanglearr(2).toDouble)
          val maxY = math.max(queryRectanglearr(1).toDouble, queryRectanglearr(3).toDouble)
          val minY = math.min(queryRectanglearr(1).toDouble, queryRectanglearr(3).toDouble)
          if (math.abs(math.max(maxX, pointStringarr(0).toDouble) - maxX) < 0.00000000000000000000001) {
            if (math.abs(math.min(minX, pointStringarr(0).toDouble) - minX) < 0.00000000000000000000001) {
              if (math.abs(math.max(maxY, pointStringarr(1).toDouble) - maxY) < 0.00000000000000000000001) {
                if (math.abs(math.min(minY, pointStringarr(1).toDouble) - minY) < 0.00000000000000000000001) {
                  true
                } else {
                  false
                }
              } else { false }

            } else {
              false
            }
          } else {
            false
          }
        } else {
          false
        }
      } else {
        false
      }
    }))

    val resultDf = spark.sql("select * from point where ST_Contains('" + arg2 + "',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains", (queryRectangle: String, pointString: String) => ({
      if (queryRectangle != null && pointString != null) {

        val queryRectanglearr = queryRectangle.split(",")
        val pointStringarr = pointString.split(",")
        if (queryRectanglearr.length == 4 || pointStringarr.length == 2) {
          val maxX = math.max(queryRectanglearr(0).toDouble, queryRectanglearr(2).toDouble)
          val minX = math.min(queryRectanglearr(0).toDouble, queryRectanglearr(2).toDouble)
          val maxY = math.max(queryRectanglearr(1).toDouble, queryRectanglearr(3).toDouble)
          val minY = math.min(queryRectanglearr(1).toDouble, queryRectanglearr(3).toDouble)
          if (math.abs(math.max(maxX, pointStringarr(0).toDouble) - maxX) < 0.00000000000000000000001) {
            if (math.abs(math.min(minX, pointStringarr(0).toDouble) - minX) < 0.00000000000000000000001) {
              if (math.abs(math.max(maxY, pointStringarr(1).toDouble) - maxY) < 0.00000000000000000000001) {
                if (math.abs(math.min(minY, pointStringarr(1).toDouble) - minY) < 0.00000000000000000000001) {
                  true
                } else {
                  false
                }
              } else { false }

            } else {
              false
            }
          } else {
            false
          }
        } else {
          false
        }
      } else {
        false
      }
    }))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within", (pointString1: String, pointString2: String, distance: Double) => ({
      if (pointString1 != null && pointString2 != null) {
        val pointString1arr = pointString1.split(",")
        val pointString2arr = pointString2.split(",")
        if (pointString1arr.length == 2 || pointString2arr.length == 2) {
          val squareddist = math.pow(pointString1arr(0).toDouble - pointString2arr(0).toDouble, 2) +
            math.pow(pointString1arr(1).toDouble - pointString2arr(1).toDouble, 2)
          val actdist = math.sqrt(squareddist).toDouble
          if ((math.abs(actdist - distance) < 0.00000000000000000000001) || (actdist < distance)) {
            true
          } else {
            false
          }
        } else {
          false
        }
      } else {
        false
      }

    }))

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
    spark.udf.register("ST_Within", (pointString1: String, pointString2: String, distance: Double) => (
      {
        if (pointString1 != null && pointString2 != null) {
          val pointString1arr = pointString1.split(",")
          val pointString2arr = pointString2.split(",")
          if (pointString1arr.length == 2 || pointString2arr.length == 2) {
            val squareddist = math.pow(pointString1arr(0).toDouble - pointString2arr(0).toDouble, 2) +
              math.pow(pointString1arr(1).toDouble - pointString2arr(1).toDouble, 2)
            val actdist = math.sqrt(squareddist).toDouble
            if ((math.abs(actdist - distance) < 0.00000000000000000000001) || (actdist < distance)) {
              true
            } else {
              false
            }
          } else {
            false
          }
        } else {
          false
        }

      }))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, " + arg3 + ")")
    resultDf.show()

    return resultDf.count()
  }
}
