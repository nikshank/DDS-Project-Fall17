package cse512

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import java.util.Calendar 

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
    {
      // Load the original data from a data source
      var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
      pickupInfo.createOrReplaceTempView("nyctaxitrips")
      //pickupInfo.show()
      // Assign cell coordinates based on pickup points
                 var now = Calendar.getInstance().getTime()
            print("1"+now + "\n")
      spark.udf.register("CalculateX", (pickupPoint: String) => ((
        HotcellUtils.CalculateCoordinate(pickupPoint, 0))))
      spark.udf.register("CalculateY", (pickupPoint: String) => ((
        HotcellUtils.CalculateCoordinate(pickupPoint, 1))))
      spark.udf.register("CalculateZ", (pickupTime: String) => ((
        HotcellUtils.CalculateCoordinate(pickupTime, 2))))
      pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
      var newCoordinateName = Seq("x", "y", "z")
      pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
      pickupInfo.createOrReplaceTempView("pickupdetails")
            now = Calendar.getInstance().getTime()
            print("2"+ now + "\n")
      //pickupInfo.show()
      //print(pickupInfo.count())

      // Define the min and max of x, y, z
      val minX = -74.50 / HotcellUtils.coordinateStep
      val maxX = -73.70 / HotcellUtils.coordinateStep
      val minY = 40.50 / HotcellUtils.coordinateStep
      val maxY = 40.90 / HotcellUtils.coordinateStep
      val minZ = 1
      val maxZ = 31
      val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)
           now = Calendar.getInstance().getTime()
            print("3"+ now + "\n")
      //   spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(HotzoneUtils.ST_Contains(queryRectangle, pointString)))
      val actualpointsDf = spark.sql("select x,y,z from pickupdetails where x>=" + minX + " and x<= " + maxX + " and y>= " + minY + " and y<= " + maxY + " and z>= " + minZ + " and z<= " + maxZ + " order by z,y,x").persist();
     // val actualpoinsSumer = actualpointsDf.first().getLong(4);
      //print(actualpoinsSumer)
            actualpointsDf.createOrReplaceTempView("actualpointsDf0")
            //spark.sql("select count(*) from actualpointsDf").show();
   
                  val actualpointsDf1 = spark.sql("select x,y,z,count(*) as totalvalue from actualpointsDf0 group by z,y,x order by z,y,x").persist();
            actualpointsDf1.createOrReplaceTempView("actualpointsDf")
            //actualpointsDf1.show()
           now = Calendar.getInstance().getTime()
            print("4"+now + "\n")

                 now = Calendar.getInstance().getTime()
            print("5"+ now + "\n")
            spark.udf.register("square", (inputX: Int) 
          => ((HotcellUtils.square(inputX))))
      val actualpointsSum = spark.sql("select count(*) as sumTotal1, sum(totalvalue) as sumTotal,sum(square(totalvalue)) as sumTotalSqr from actualpointsDf");
      actualpointsSum.createOrReplaceTempView("actualpointsSum")
      //actualpointsSum.show()
                       now = Calendar.getInstance().getTime()
            print("6"+ now + "\n")
      //print(actualpointsSum + "%")
      val actualpoinsSumex = actualpointsSum.first().getLong(1);
      val actualpoinsSumsquaresex = actualpointsSum.first().getDouble(2);
     val actualpoinsSumsquaresexer = actualpointsSum.first().getLong(0);
            print(" % "+actualpoinsSumex+" % " +actualpoinsSumsquaresex + " % " +actualpoinsSumsquaresexer)
      val mean =(actualpoinsSumex.toDouble / numCells.toDouble).toDouble;
      val SD = math.sqrt(((actualpoinsSumsquaresex.toDouble / numCells.toDouble) - (mean.toDouble * mean.toDouble))).toDouble
      print(" % "+mean +" % "+ SD +" % ")
            spark.udf.register("FindNeighbors", (inputX: Int, inputY: Int, inputZ: Int, maxX: Int, maxY: Int, maxZ: Int, minX: Int, minY: Int, minZ: Int) 
          => ((HotcellUtils.FindNoofNeighbors(inputX, inputY, inputZ, maxX, maxY, maxZ, minX, minY, minZ))))
      val NeighBors = spark.sql("select FindNeighbors(a1.x,a1.y,a1.z," + maxX + "," + maxY + "," + maxZ + "," + minX + "," + minY + "," + minZ + ") as nCount,count(*) as county, a1.x as x,a1.y as y,a1.z as z, sum(a2.totalvalue) as sumtotalvalneigh from actualpointsDf as a1, actualpointsDf as a2 where (a2.x = a1.x+1 or a2.x = a1.x or a2.x =a1.x-1) and (a2.y = a1.y+1 or a2.y = a1.y or a2.y =a1.y-1) and (a2.z = a1.z+1 or a2.z = a1.z or a2.z =a1.z-1) group by a1.z,a1.y,a1.x order by a1.z,a1.y,a1.x").persist()
      NeighBors.createOrReplaceTempView("FinalNeighborhood");
                       now = Calendar.getInstance().getTime()
            print("8"+ now + "\n")
                  spark.udf.register("GettheOddStat", (x: Int, y: Int, z: Int, mean:Double, sd: Double, noNeighbor: Int, sumNeighbor: Int, numcells: Int) => ((
        HotcellUtils.gettisordstatistic(x, y, z, mean, sd, noNeighbor, sumNeighbor, numcells))))
      val Neighbors3 = spark.sql("select GettheOddStat(x,y,z,"+mean+","+SD+",ncount,sumtotalvalneigh,"+numCells+") as gtstat,x, y, z from FinalNeighborhood order by gtstat desc");
      Neighbors3.createOrReplaceTempView("JioMereStat")
// Neighbors3.show()
      val finalresult = spark.sql("select x,y,z,gtstat from JioMereStat")
      finalresult.createOrReplaceTempView("hogayaatlast")
                       now = Calendar.getInstance().getTime()
            print("9"+ now + "\n")
      return finalresult // YOU NEED TO CHANGE THIS PART
    }
}