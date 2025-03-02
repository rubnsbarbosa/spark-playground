package com.playground.sandbox

import org.apache.spark.sql.SparkSession
import com.playground.spark.joins.PlayJoins._
import com.playground.spark.dataset.PlayDataSet._
import com.playground.spark.dataframe.PlayDataFrame._

class Sandbox(sparkSession: SparkSession) extends Serializable {
    private val spark: SparkSession = sparkSession

    def run(): Unit = {
        fetchStarWarsCharacters()
        fetchStarWarsPlanets()
        fetchStarWarsStarships()
        fetchStarWarsSpecies()

        handleLeftOuterJoin()
        handleInnerJoin()
        handleJoinWithFilter()

        fetchCharactersDataset()
        fetchPlanetsDataset()
        fetchStarshipDataset()
        fetchSpeciesDataset()
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

    private def handleJoinWithFilter(): Unit = {
        val characters = getStarWarsCharactersDataFrame(spark)
        val planets = getStarWarsPlanetsDataFrame(spark)

        val joinWithOrbitalPeriodFilter = joinWithFilterInOrbitalPeriod(characters, planets)
        joinWithOrbitalPeriodFilter.show()
    }

    private def fetchCharactersDataset(): Unit = {
        val characters = getStarWarsCharactersDataFrame(spark)
        val charactersDS = getStarWarsCharactersDataset(spark, characters)
        charactersDS.show()
    }

    private def fetchPlanetsDataset(): Unit = {
        val planets = getStarWarsPlanetsDataFrame(spark)
        val planetsDS = getStarWarsPlanetsDataset(spark, planets)
        planetsDS.show()
    }

    private def fetchStarshipDataset(): Unit = {
        val starships = getStarWarsStarshipsDataFrame(spark)
        val starshipsDS = getStarWarsStarshipsDataset(spark, starships)
        starshipsDS.show()
    }

    private def fetchSpeciesDataset(): Unit = {
        val species = getStarWarsSpeciesDataFrame(spark)
        val speciesDS = getStarWarsSpeciesDataset(spark, species)
        speciesDS.show()
    }
}
