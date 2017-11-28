package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
    {
      // Configuration variable:
      // Coordinate step is the size of each cell on x and y
      var result = 0
      coordinateOffset match {
        case 0 => result = Math.floor((inputString.split(",")(0).replace("(", "").toDouble / coordinateStep)).toInt
        case 1 => result = Math.floor(inputString.split(",")(1).replace(")", "").toDouble / coordinateStep).toInt
        // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
        case 2 => {
          val timestamp = HotcellUtils.timestampParser(inputString)
          result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
        }
      }
      return result
    }
  
  def square(a:Int):Double=
  {
    return (a*a).toDouble;
  }
    def CalculateIsNeighbor(a: Int, b: Int): Boolean =
    {
      if(math.abs(a-b) <=1)
      {
        return true
      }
      return false
    }
    
        def CalculateIsNeighboradv(a: Int, b: Int,c:Int,d:Int,e:Int,f:Int): Boolean =
    {
      if(math.abs(a-b) <=1)
      {
        return true
      }
      return false
    }
    
  def FindNoofNeighbors(inputX: Int, inputY: Int, inputZ: Int, maxX: Int, maxY: Int, maxZ: Int,minX: Int, minY: Int, minZ: Int): Int =
    {
      var noofTermindexes = 0;
      val val7 = 7
      val val17 =17
      val val26 =26
      val val11 =11
      //var arr = Array.ofDim[Int](math.abs(maxX-minX+1),math.abs(maxY-minY+1),math.abs(maxZ-minZ+1))
      
      if (inputX == minX || inputX == maxX) {
        noofTermindexes += 1;
      }

      if (inputY == minY || inputY == maxY) {
        noofTermindexes += 1;
      }

      if (inputZ == minZ || inputZ == maxZ) {
        noofTermindexes += 1;
      }

      if (noofTermindexes == 1) {
        
        return val17;
      } else 
        if (noofTermindexes == 2)
      {
        return val11;
      }
      else if (noofTermindexes == 3)
        
        {return val7;
        }
        
      else
      {
        return val26;
      }

      //return noofTermindexes;
    }
  def gettisordstatistic(x: Int, y: Int, z: Int, mean:Double, sd: Double, noNeighbor: Int, sumNeighbor: Int, numcells: Int): Double =
  {
    val numerator = (sumNeighbor.toDouble - (mean*noNeighbor.toDouble))
    val denominator = sd*math.sqrt((((numcells.toDouble*noNeighbor.toDouble) -(noNeighbor.toDouble*noNeighbor.toDouble))/(numcells.toDouble-1.0).toDouble).toDouble).toDouble
    return (numerator/denominator).toDouble
  }

  def timestampParser(timestampString: String): Timestamp =
    {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val parsedDate = dateFormat.parse(timestampString)
      val timeStamp = new Timestamp(parsedDate.getTime)
      return timeStamp
    }
  def dayOfYear(timestamp: Timestamp): Int =
    {
      val calendar = Calendar.getInstance
      calendar.setTimeInMillis(timestamp.getTime)
      return calendar.get(Calendar.DAY_OF_YEAR)
    }

  def dayOfMonth(timestamp: Timestamp): Int =
    {
      val calendar = Calendar.getInstance
      calendar.setTimeInMillis(timestamp.getTime)
      return calendar.get(Calendar.DAY_OF_MONTH)
    }
  //  def ST_ContainsforHotCells(queryRectangle: String, pointString: String): Boolean = {
  //    if (queryRectangle != null && pointString != null) {
  //
  //      val queryRectanglearr = queryRectangle.split(",")
  //      val pointStringarr = pointString.split(",")
  //      if (queryRectanglearr.length == 6 || pointStringarr.length == 3) {
  //        val maxX = math.max(queryRectanglearr(0).toDouble, queryRectanglearr(2).toDouble)
  //        val minX = math.min(queryRectanglearr(0).toDouble, queryRectanglearr(2).toDouble)
  //        val maxY = math.max(queryRectanglearr(1).toDouble, queryRectanglearr(3).toDouble)
  //        val minY = math.min(queryRectanglearr(1).toDouble, queryRectanglearr(3).toDouble)
  //        val maxZ = 31
  //        val minZ = 1
  //        if (math.abs(math.max(maxX, pointStringarr(0).toDouble) - maxX) < 0.00000000000000000000001) {
  //          if (math.abs(math.min(minX, pointStringarr(0).toDouble) - minX) < 0.00000000000000000000001) {
  //            if (math.abs(math.max(maxY, pointStringarr(1).toDouble) - maxY) < 0.00000000000000000000001) {
  //              if (math.abs(math.min(minY, pointStringarr(1).toDouble) - minY) < 0.00000000000000000000001) {
  //                if (math.abs(math.max(maxZ, pointStringarr(2).toDouble) - maxZ) < 0) {
  //                  if (math.abs(math.min(minZ, pointStringarr(2).toDouble) - minZ) < 0) {
  //                    return true
  //                  } else {
  //                    return false
  //                  }
  //                } else {
  //                  return false
  //                }
  //
  //              } else {
  //                return false
  //              }
  //            } else { return false }
  //
  //          } else {
  //            return false
  //          }
  //        } else {
  //          return false
  //        }
  //      } else {
  //        return false
  //      }
  //    } else {
  //      return false
  //    }
  //    // YOU NEED TO CHANGE THIS PART
  //    //return true // YOU NEED TO CHANGE THIS PART
  //  }
  // YOU NEED TO CHANGE THIS PART
}
