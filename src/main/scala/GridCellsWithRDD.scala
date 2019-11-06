import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Run {

  val xRange = 10000
  val yRange = 10000
  val window = 20
  val considerCommonGrids = false
  val pointsFile = "/Users/badgod/badgod_documents/github/BigDataTutorials/input/project2/xy_coordinates_big.txt"


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
      for (y <- 0 to yRange - 1 by window) {
        for (x <- 0 to xRange - 1 by window) {
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
    val pointsDFFloat = spark.sqlContext.read.format("csv").option("header", "true").schema(dfSchema()).load(pointsFile)
    val pointsRDD = pointsDFFloat.withColumn("x", col("x").cast(IntegerType)).withColumn("y", col("y").cast(IntegerType)).rdd
    var grids = createGrids()
    var gridNeighbours = findNeighbours(grids)
    println("Reading data done, found grids and neighbours")

    val withGridRDD = pointsRDD.map(point => {
      val a = point(0).toString.toInt
      val b = point(1).toString.toInt
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
    })

    val gridCountRDD = withGridRDD.map(grid => (grid, 1)).reduceByKey((acc, x) => (acc + x))
    val gridCountRDDMap = gridCountRDD.collectAsMap()

    val withNeighbours = gridCountRDD.map(line => {
      val neighbours = gridNeighbours(line._1)
      val total = neighbours.split("/").map(grid => gridCountRDDMap(grid)).sum
      val count = neighbours.split("/").size
      val average = total / count
      (line._1, line._2, neighbours, average, line._2 / average.toFloat)
    })

    val sortedTop50 = withNeighbours.sortBy(_._5, ascending = false).take(50)
    val gridRDIMap = withNeighbours.map(line => (line._1, line._5)).collectAsMap()

    val finalRDD = sortedTop50.map(line => {
      val neighbours = line._3.split("/")
      val neighboursData = neighbours.map(grid => ("{" + grid + ":" + gridRDIMap(grid) + "}")).toList
      (line._1, line._5, neighboursData.toString())
    })

    finalRDD.foreach(line => println("GRID: " + line._1 + " RDI: " + line._2 + " || Neighbours: " + line._3))
    println("Time taken in seconds : " + (System.currentTimeMillis() - start) / 1000)
  }
}