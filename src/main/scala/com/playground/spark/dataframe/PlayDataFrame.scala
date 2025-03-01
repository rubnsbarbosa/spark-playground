package com.playground.spark.dataframe

import com.playground.models.{Customer, Products}
import org.apache.spark.sql.{DataFrame, SparkSession}

object PlayDataFrame {
    def getCustomerDataFrame(spark: SparkSession): DataFrame = {
        val customerDataFrame = spark.createDataFrame(
            Customer(1, "CUSTOMER-001", 21)::
            Customer(2, "CUSTOMER-002", 22)::
            Customer(3, "CUSTOMER-003", 23)::
            Customer(4, "CUSTOMER-004", 24)::
            Customer(5, "CUSTOMER-005", 25):: Nil
        )
        customerDataFrame
    }

    def getProductsDataFrame(spark: SparkSession): DataFrame = {
        val productsDataFrame = spark.createDataFrame(
                Products(123, 1, "Keyboard")::
                Products(321, 2, "Mouse"):: Nil
            )
        productsDataFrame
    }

    def getStarWarsCharactersDataFrame(spark: SparkSession): DataFrame = {
        import spark.implicits._

        val starWarsCharacters = Seq(
            (1, "Luke Skywalker", "172", "77", "19BBY", "male", "https://swapi.dev/api/people/1/"),
            (2, "C-3PO", "167", "75", "112BBY", null, "https://swapi.dev/api/people/2/"),
            (3 ,"R2-D2", "96", "32", "33BBY", null, "https://swapi.dev/api/people/3/"),
            (4, "Darth Vader", "202", "136", "41.9BBY", "male", "https://swapi.dev/api/people/4/"),
            (5, "Leia Organa", "150", "49", "19BBY", "female", "https://swapi.dev/api/people/5/")
        ).toDF("id", "name", "height", "mass", "birth_year", "gender", "url")
        starWarsCharacters
    }

    def getStarWarsPlanetsDataFrame(spark: SparkSession): DataFrame = {
        import spark.implicits._

        val starWarsPlanets = Seq(
            (1, "Tatooine", "23", "304", "10465", "arid", "desert", "200000", "https://swapi.dev/api/planets/1"),
            (2, "Alderaan", "24", "364", "12500", "temperate", "mountains", "2000000000", "https://swapi.dev/api/planets/2"),
            (3, "Yavin IV", "24", "4818", "10200", "tropical", "rainforests", "1000", "https://swapi.dev/api/planets/3"),
            (4, "Hoth", "23", "549", "7200", "frozen", "tundra", "unknown", "https://swapi.dev/api/planets/4"),
            (5, "Dagobah", "23", "341", "8900", "murky", "jungles", "unknown", "https://swapi.dev/api/planets/5")
        ).toDF("id", "name", "rotation_period", "orbital_period", "diameter", "climate", "terrain", "population", "url")
        starWarsPlanets
    }

    def getStarWarsStarshipsDataFrame(spark: SparkSession): DataFrame = {
        import spark.implicits._

        val starWarsStarships = Seq(
            ("CR90 corvette", "CR90 corvette", "Corellian Engineering Corporation", "150", "950", "600", "3000000"),
            ("Star Destroyer", "Imperial I-class Star Destroyer", "Kuat Drive Yards", "1600", "975", null, "36000000"),
            ("Sentinel-class landing craft", "Sentinel-class landing craft", "Sienar Fleet Systems, Cyngus Spaceworks", "38", "1000", "75", "180000"),
            ("Death Star", "DS-1 Orbital Battle Station", "Imperial Department of Military Research, Sienar Fleet Systems", "120000", null, "843342", "1000000000000"),
            ("Millennium Falcon", "YT-1300 light freighter", "Corellian Engineering Corporation", "34.37", "1050", "6", "100000"),
            ("Y-wing", "BTL Y-wing", "Koensayr Manufacturing", "14", "100", "0", "110"),
            ("X-wing", "T-65 X-wing", "Incom Corporation", "12.5", "1050", "0", "110"),
            ("TIE Advanced x1", "Twin Ion Engine Advanced x1", "Sienar Fleet Systems", "9.5", "1200", "0", "150"),
            ("Executor", "Executor-class star dreadnought", "Kuat Drive Yards, Fondor Shipyards", "19000", null, "38000", "250000000"),
            ("Rebel transport", "GR-75 medium transport", "Gallofree Yards, Inc.", "90", "650", "90", "19000000"),
            ("Imperial shuttle", "Lambda-class T-4a shuttle", "Sienar Fleet Systems", "20", "850", "20", "80000")
        ).toDF("name", "model", "manufacturer", "length", "max_atmosphere_speed", "passengers", "cargo_capacity")
        starWarsStarships
    }

    def getStarWarsSpeciesDataFrame(spark: SparkSession): DataFrame = {
        import spark.implicits._

        val starWarsSpecies = Seq(
            ("Human", "mammal", "sentient", "180", "120", "Galatic Basic"),
            ("Droid", "artificial", "sentient", null, "indefinite", null),
            ("Wookie", "mammal", "sentient", "210", "400", "Galatic Basic"),
            ("Rodian", "sentient", "reptilian", "170", null, "Galatic Basic"),
            ("Hutt", "gastropod", "sentient", "300", "1000", "Huttese"),
            ("Yoda's species", "mammal", "sentient", "66", "900", "Galactic basic"),
            ("Trandoshan", "reptile", "sentient", "200", null, "Dosh"),
            ("Mon Calamari", "amphibian", "sentient", "160", null, "Mon Calamarian"),
            ("Ewok", "mammal", "sentient", "100", null, "Ewokese"),
            ("Sullustan", "mammal", "sentient", "180", null, "Sullutese"),
            ("Neimodian", null, "sentient", "180", null, "Neimoidia"),
            ("Gungan", "amphibian", "sentient", "190", null, "Gungan basic"),
            ("Toydarian", "mammal", "sentient", "120", "91", "Toydarian"),
        ).toDF("name", "classification", "designation", "average_height", "average_lifespan", "language")
        starWarsSpecies
    }

}
