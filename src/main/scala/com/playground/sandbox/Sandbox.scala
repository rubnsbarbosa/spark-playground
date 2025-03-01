package com.playground.sandbox

import com.playground.spark.dataframe.PlayDataFrame._
import com.playground.spark.joins.PlayJoins.joinCustomersProducts
import org.apache.spark.sql.SparkSession

class Sandbox(sparkSession: SparkSession) extends Serializable {
    private val spark: SparkSession = sparkSession

    def run(): Unit = {
        fetchStarWarsDataFrames()
        handleLeftOuterJoin()
    }

    private def fetchStarWarsDataFrames(): Unit = {
        val swDF = getStarWarsDataFrame(spark)
        swDF.show(truncate = false)
    }

    private def handleLeftOuterJoin(): Unit = {
        val customerDataFrame = getCustomerDataFrame(spark)
        val productsDataFrame = getProductsDataFrame(spark)

        val leftOuterJoinResult = joinCustomersProducts(customerDataFrame, productsDataFrame)
        leftOuterJoinResult.show()
    }

}
