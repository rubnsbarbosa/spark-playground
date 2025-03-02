package com.playground.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PlayRdd {
    private def createRddList(spark: SparkSession): RDD[Int] = {
        val rddListNumbers: RDD[Int] = spark.sparkContext.parallelize(
            List(1, 2, 3, 4, 5)
        )
        rddListNumbers
    }

    def createRddSquares(spark: SparkSession): Unit = {
        // create a new rdd, it'll square all elements
        val rdd = createRddList(spark)

        val rddSquaredAllElements = rdd.map(
            element => element * element
        )
        print(rddSquaredAllElements.collect().mkString(","))
        // output: 1,4,9,16,25
    }
}
