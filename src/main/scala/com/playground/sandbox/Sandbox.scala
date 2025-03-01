package com.playground.sandbox

import com.playground.spark.dataframe.PlayDataFrame._
import com.playground.spark.joins.PlayJoins._
import org.apache.spark.sql.SparkSession

class Sandbox(sparkSession: SparkSession) extends Serializable {
    private val spark: SparkSession = sparkSession

    def run(): Unit = {
        fetchStarWarsCharacters()
        fetchStarWarsPlanets()
        fetchStarWarsStarships()
        fetchStarWarsSpecies()

        handleLeftOuterJoin()
        handleInnerJoin()
    }

    private def fetchStarWarsCharacters(): Unit = {
        val swDF = getStarWarsCharactersDataFrame(spark)
        swDF.show(truncate = false)
    }

    private def fetchStarWarsPlanets(): Unit = {
        val swDF = getStarWarsPlanetsDataFrame(spark)
        swDF.show(truncate = false)
    }

    private def fetchStarWarsStarships(): Unit = {
        val swDF = getStarWarsStarshipsDataFrame(spark)
        swDF.show(truncate = false)
    }

    private def fetchStarWarsSpecies(): Unit = {
        val swDF = getStarWarsSpeciesDataFrame(spark)
        swDF.show(truncate = false)
    }

    private def handleLeftOuterJoin(): Unit = {
        val customerDataFrame = getCustomerDataFrame(spark)
        val productsDataFrame = getProductsDataFrame(spark)

        val leftOuterJoinResult = leftOuterJoinCustomersProducts(customerDataFrame, productsDataFrame)
        leftOuterJoinResult.show()
    }

    private def handleInnerJoin(): Unit = {
        val customerDataFrame = getCustomerDataFrame(spark)
        val productsDataFrame = getProductsDataFrame(spark)

        val innerJoinResult = innerJoinCustomersProducts(customerDataFrame, productsDataFrame)
        innerJoinResult.show()
    }

}
