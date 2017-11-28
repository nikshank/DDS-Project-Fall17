package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
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
                  return true
                } else {
                  return false
                }
              } else { return false }

            } else {
              return false
            }
          } else {
           return false
          }
        } else {
         return false
        }
      } else {
       return false
      }
    // YOU NEED TO CHANGE THIS PART
    //return true // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}
