import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object SparkBasicDFOperations {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark data transformations")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /*

    // basics
    val arr = Array(1, 2, 3, 4, 5)
    val newRDD = sc.parallelize(arr) // creates RDD
    newRDD.first() // first object in rdd
    newRDD.take(10).foreach(println) // print first 10 lines/values in rdd
    newRDD.collect() // get all elements in rdd
    newRDD.collect().foreach(println) // print each element in new line
    println(newRDD.partitions.size) // gives the number of partitions newRDD is split into
    // each partition gets executed in each core in a machine. So more the number of cores better parallelization can be achieved

    // How to create RDD using files
    val fileRDD = sc.textFile("hdfs://localhost:9000/ds503/hw1/input/transactions.txt")

    // RDD transformations

    // Notes:
    // Spark useless lazy evaluation
    // Every transformation creates a new RDD from the exisitng RDD after applying the specified transformation

    // filtering each line in fileRDD and creating a new RDD using filter operation.
    // Condition is length of each line should be greater than 20.
    val filterRDDByLength = fileRDD.filter(line => line.length > 20)

    // takes each line in fileRDD, splits it by delimiter and returns an array. Final output is array of arrays (first array is number of lines in fileRDD, inner array is created based on split operation)
    // So map takes an array and creates array of arrays
    // Map takes each line and applies a given function to it
    val mapRDD = fileRDD.map(line => line.split(","))

    // similar to map, but flattens the array of arrays into a single array
    val flatMapRDD = fileRDD.flatMap(line => line.split(","))

    // to get distinct elements from an RDD
    val distinctRDD = newRDD.distinct()

    // Filter lines
    val filterRDDLines = fileRDD.filter(line => line != "some_value")
    val filterRDDLinesAlternativeWay = fileRDD.filter(_ != "some_value")

    // writing function inside map
    fileRDD.map(line => {
      val values = line.split(",")
      (values(0), values(1), values(2)) // tuple
      // Array(values(0), values(1), values(2)).mkString(",") // Instead of tuple we can use array. mkstirng is used to stringify array and print
      // List(values(0), values(1), values(2)).mkString(",") // Instead of tuple we can use List
    }).take(10).foreach(println)
     */

    val transactionFile = args(0)
    val df = spark.read.csv(transactionFile).toDF("trans_id", "cust_id", "transaction_amount", "num_of_items", "desc")
    println("df.count()", df.count())
    println("*************** T1 STARTS ***************")
    val t1Result = df.filter(col("transaction_amount") >= 200)
    t1Result.show(truncate = false)
    println("*************** T1 ENDS ***************")


    println("*************** T2 STARTS ***************")
    // T2 query
    // Over T1, group the transactions by the Number of Items it has, and for each group
    // calculate the sum of total amounts, the average of total amounts, the min and the max of
    // the total amounts
    val t2Result = t1Result.groupBy("num_of_items").agg(
      sum("transaction_amount") as "total_transaction_amount",
      sum("transaction_amount") / count("transaction_amount") as "amount_average",
      min("transaction_amount") as "min_amount",
      max("transaction_amount") as "max_amount"
    )
    t2Result.show(truncate = false)
    println("*************** T2 ENDS ***************")


    println("*************** T3 STARTS ***************")
    // T3: Over T1, group the transactions by customer ID, and for each group report the
    // customer ID, and the transactions’ count.
    val t3Result = t1Result.groupBy("cust_id").agg(
      count("*") as "transactions_count_t3"
    )
    t3Result.show(truncate = false)
    println("*************** T3 ENDS ***************")


    println("*************** T4 STARTS ***************")
    // T4: Filter out (drop) the transactions from T whose total amount is less than $600
    val t4Result = df.filter(col("transaction_amount") >= 600)
    t4Result.show(truncate = false)
    println("*************** T4 ENDS ***************")


    println("*************** T5 STARTS ***************")
    // T5: Over T4, group the transactions by customer ID, and for each group report the
    //customer ID, and the transactions’ count.
    val t5Result = t4Result.groupBy("cust_id").agg(
      count("*") as "transactions_count_t5"
    )
    t5Result.show(truncate = false)
    println("*************** T5 ENDS ***************")


    println("*************** T6 STARTS ***************")
    // T6: Select the customer IDs whose T5.count * 5 < T3.count
    val joinDF = t3Result.join(t5Result, usingColumn = "cust_id")
    val t6Result = joinDF.filter(col("transactions_count_t5") * 5 < col("transactions_count_t3")).select("cust_id")
    t6Result.show(truncate = false)
    println("*************** T6 ENDS ***************")
  }


}