import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object GridCellRun {

  val xRange = 10000
  val yRange = 10000
  val window = 500


  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    // read dataset
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark data transformations")
      .getOrCreate()
    val sc = spark.sparkContext

    sc.setLogLevel("ERROR")

    def dfSchema(): StructType = StructType(Seq(
      StructField(name = "x", dataType = FloatType, nullable = false),
      StructField(name = "y", dataType = FloatType, nullable = false)))

    def createData(xRange: Int, yRange: Int, numOfPoints: Int): ListBuffer[List[Int]] = {
      val points = new ListBuffer[List[Int]]()
      val randomGenerator = scala.util.Random
      for (x <- 0 to numOfPoints - 1) {
        points += List(randomGenerator.nextInt(xRange), randomGenerator.nextInt(yRange))
      }
      points
    }

    // if creating data from scratch

    //    val numOfPoints = 100
    //    val points = createData(xRange, yRange, numOfPoints)
    //    val pointsRDD = sc.parallelize(points)
    //

    //
    //    def row(line: List[Int]): Row = Row(line(0), line(1))
    //
    //    val schema = dfSchema()
    //    val data = pointsRDD.map(x => x).map(row)
    //    val pointsDF = spark.createDataFrame(data, schema)
    //    pointsDF.show()
    // data creation done

    def createGrids(): mutable.HashMap[String, String] = {
      var gripMap = new mutable.HashMap[String, String]()
      var count = 1
      for (x <- 0 to xRange - 1 by window) {
        for (y <- 0 to yRange - 1 by window) {
          if (x <= xRange && y <= yRange) {
            var key = List(x, y, x + window, y + window).mkString("_")
            gripMap.put(key, "grid_" + count)
            count += 1
          }
        }
      }
      gripMap
    }

    def findNeighbours(grids: mutable.HashMap[String, String]): mutable.HashMap[String, String] = {
      var neighbourMap = new mutable.HashMap[String, String]()

      grids.keys.foreach(key => {
        val localNeighbours = new ListBuffer[String]()
        val values = key.split("_")
        val newX1 = scala.math.max(0, values(0).toInt - window)
        val newY1 = scala.math.max(0, values(1).toInt - window)
        val newX2 = scala.math.min(values(2).toInt + window, xRange)
        val newY2 = scala.math.min(values(3).toInt + window, yRange)

        for (x <- newX1 to newX2 - 1 by window) {
          for (y <- newY1 to newY2 - 1 by window) {
            if (x + window <= newX2 && y + window <= newY2) {
              var gridCordinates = List(x, y, x + window, y + window).mkString("_")
              if (!gridCordinates.equals(key)) {
                localNeighbours += grids(gridCordinates)
              }
            }
          }
        }
        neighbourMap(grids(key)) = localNeighbours.mkString("/")
      })
      neighbourMap
    }

    // if loading data from file
    val pointsFile = "/Users/badgod/badgod_documents/github/BigDataTutorials/input/project2/xy_coordinates_big.txt"
    val pointsDFFloat = spark.sqlContext.read.format("csv").option("header", "true").schema(dfSchema()).load(pointsFile)
    val pointsDF = pointsDFFloat.withColumn("x", col("x").cast(IntegerType)).withColumn("y", col("y").cast(IntegerType))
    val pointsRDD = pointsDFFloat.withColumn("x", col("x").cast(IntegerType)).withColumn("y", col("y").cast(IntegerType)).rdd
    var grids = createGrids()
    var gridNeighbours = findNeighbours(grids)
    println("Reading data done, found grids and neighbours")

    def pointInGrid(a: Int, b: Int, x1: Int, y1: Int, x2: Int, y2: Int) = { //x1 < x < x2 and y1 < y < y2
      (x1 <= a) && (a <= x2) && (y1 <= b) && (b <= y2)
    }

    def associatePointToGridUDF(grids: mutable.HashMap[String, String]) = udf { cooradinates: String =>
      val coords = cooradinates.split(",")
      val a = coords(0).toInt
      val b = coords(1).toInt
      val modValX = a % window
      var x1 = a - modValX
      var x2 = x1 + window
      val modValY = b % window
      var y1 = b - modValY
      var y2 = y1 + window
      if (x2 > xRange) {
        x2 = x1
        x1 = x1 - window
      }
      if (y2 > yRange) {
        y2 = y1
        y1 = y1 - window
      }
      grids(List(x1, y1, x2, y2).mkString("_"))
      //      x1 = x1 - (2 * window)
      //      x2 = x2 + (2 * window)
      //      y1 = y1 - (2 * window)
      //      y2 = y2 + (2 * window)
      //      val grid_ids = new ListBuffer[String]()
      //      for (x <- x1 to x2 - 1 by window) {
      //        for (y <- y1 to y2 - 1 by window) {
      //          if ((x <= x2 && y <= y2) && (x >= 0 && y >= 0 && x + window <= xRange && y + window <= yRange)) {
      //            if (pointInGrid(a, b, x, y, x + window, y + window)) {
      //              grid_ids += grids(List(x, y, x + window, y + window).mkString("_"))
      //            }
      //          }
      //        }
      //      }
      //      grid_ids
    }

    val associatePointToGrid = associatePointToGridUDF(grids)
    //    val gridDF = pointsDF.withColumn("grid", associatePointToGrid(concat(col("x"), lit(","), col("y"))))
    //      .drop("x", "y")
    //      .withColumn("separate_grids", explode(col("grid")))
    //      .drop("grid")

    val gridDF = pointsDF.withColumn("separate_grids", associatePointToGrid(concat(col("x"), lit(","), col("y"))))
      .drop("x", "y")

    val gridDFCount = gridDF.groupBy("separate_grids").agg(count("*") as "XCount")


    def getNeighboursUDF(gridNeighbours: mutable.HashMap[String, String]) = udf { grid: String =>
      gridNeighbours(grid)
    }

    val createNeighbours = getNeighboursUDF(gridNeighbours)
    val neighboursRDD = gridDFCount.withColumn("neighbours", createNeighbours(col("separate_grids")))
    println("Assigned neighbours to grids")


    val neighbourDFWithCountAsMap = neighboursRDD.rdd.map(row => (row.getString(0) -> row.getLong(1))).collectAsMap()

    def getCount(neighbourDFWithCountAsMap: scala.collection.Map[String, Long]) = udf { neighbourGridsStr: String =>
      var sum = 0L
      var count = 0
      for (neighbourGridId <- neighbourGridsStr.split("/")) {
        sum += neighbourDFWithCountAsMap.getOrElse(neighbourGridId, 0L)
        count += 1
      }
      sum / count
    }

    val getCountUDF = getCount(neighbourDFWithCountAsMap)
    val neighboursRDDWithYCount = neighboursRDD.withColumn("YCount", getCountUDF(col("neighbours")))

    println("Calculated neighbours count (YCount)")


    val densityDF = neighboursRDDWithYCount.withColumn("grid_rdi", col("XCount") / col("YCount")).orderBy(desc("grid_rdi"))
    println("Calculated grid relative index")

    // step 3
    val relativeIndexTop50 = densityDF.select("separate_grids", "grid_rdi", "neighbours").limit(50)
    val relativeIndexMap = densityDF.select("separate_grids", "grid_rdi").rdd.map(row => (row.getString(0) -> row.getDouble(1))).collectAsMap()

    def getRIforNeighbours(relativeIndexTop50Map: scala.collection.Map[String, Double]) = udf { neighbourGridsStr: String =>
      var ri = new ListBuffer[String]()
      for (neighbourGridId <- neighbourGridsStr.split("/")) {
        ri += BigDecimal(relativeIndexTop50Map.getOrElse(neighbourGridId, 0d)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toString
      }
      ri.mkString("/")
    }

    val getRIforNeighboursUDF = getRIforNeighbours(relativeIndexMap)
    val dfWithTop50NeighboursRI = relativeIndexTop50.withColumn("neighbours_rdi", getRIforNeighboursUDF(col("neighbours")))
    println("Calculated neighbour grid relative index")
    dfWithTop50NeighboursRI.show(truncate = false)
    println("Time taken " + (System.currentTimeMillis() - start) / 1000)
  }
}