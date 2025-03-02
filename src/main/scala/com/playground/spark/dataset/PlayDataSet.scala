package com.playground.spark.dataset

import com.playground.models._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object PlayDataSet {
    def getStarWarsCharactersDataset(spark: SparkSession, characters: DataFrame): Dataset[StarWarsCharacters] = {
        import spark.implicits._

        val charactersDataset = characters
            .where(
                col("id").isNotNull
            )
            .select(
                col("id"),
                col("name"),
                col("height"),
                col("mass"),
                col("birth_year"),
                col("gender"),
                col("planet_visited_id"),
                col("url")
            )
            .as[StarWarsCharacters]

        charactersDataset
    }

    def getStarWarsPlanetsDataset(spark: SparkSession, planets: DataFrame): Dataset[StarWarsPlanets] = {
        import spark.implicits._

        val planetsDataset = planets
            .where(
                col("planet_id").isNotNull
            )
            .select(
                col("planet_id"),
                col("p_name"),
                col("rotation_period"),
                col("orbital_period"),
                col("diameter"),
                col("climate"),
                col("terrain"),
                col("population"),
                col("url")
            )
            .as[StarWarsPlanets]

        planetsDataset
    }

    def getStarWarsStarshipsDataset(spark: SparkSession, starships: DataFrame): Dataset[StarWarsStarships] = {
        import spark.implicits._

        val starshipsDataset = starships
            .select(
                col("name"),
                col("model"),
                col("manufacturer"),
                col("length"),
                col("max_atmosphere_speed"),
                col("passengers"),
                col("cargo_capacity")
            )
            .as[StarWarsStarships]

        starshipsDataset
    }

    def getStarWarsSpeciesDataset(spark: SparkSession, species: DataFrame): Dataset[StarWarsSpecies] = {
        import spark.implicits._

        val speciesDataset = species
            .select(
                col("name"),
                col("classification"),
                col("designation"),
                col("average_height"),
                col("average_lifespan"),
                col("language")
            )
            .as[StarWarsSpecies]

        speciesDataset
    }
}
