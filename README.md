# Spark in Scala

#### This repo contains basic examples of how to work with Spark RDD and Spark DataFrames in Scala

 - How to create RDD using arrays and some basic api usages
    
        val arr = Array(1,2,3,4,5)
        val newRDD = sc.parallelize(arr) --> creates RDD    
        newRDD.first() -> first object in rdd
        newRDD.take(n).foreach(println) -> print first n objects in rdd
        newRDD.collect() -> get all elements in rdd
        newRDD.collect().foreach(println) -> print each element in new line
        newRDD.partitions.size -> gives the number of partitions newRDD is split into
    
    each partition gets executed in each core in a machine. So more the number of cores better parallelization can be achieved

 - How to create RDD using files

        val fileRDD = sc.textFile('path')
    
 - RDD transformations
    
        Notes:
            Spark uses lazy evaluation
            Every transformation creates a new RDD from the exisitng RDD after applying the specified transformation
            
            ## filtering each line in fileRDD and creating a new RDD using filter operation.
            ## Condition is length of each line should be greater than 20. 
            val filterRDD = fileRDD.filter(line => line.length > 20)
            
            # takes each line in fileRDD, splits it by delimiter and returns an array. Final output is array of arrays (first array is number of lines in fileRDD, inner array is created based on split operation)
            # So map takes an array and creates array of arrays
            # Map takes each line and applies a given function to it
            val mapRDD = fileRDD.map(line => line.split(",")) 
            
            # similar to map, but flattens the array of arrays into a single array
            val flatMapRDD = fileRDD.flatMap(line => line.split(","))
            
            # to get distinct elements from an RDD
            val distinctRDD = newRDD.distinct()
            
            # Filter lines
            val filterRDD = fileRDD.filter(line=>line!="some_value")
            val filterRDD = fileRDD.filter(_!="some_value")
            
    
 Happy learning!!
