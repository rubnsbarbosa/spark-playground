package com.playground.spark.joins

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object PlayJoins {
    def leftOuterJoinCustomersProducts(customers: DataFrame, products: DataFrame): DataFrame = {
        val leftOuterJoinDataFrame = customers
            .join(products, col("customer_identifier") === col("customer_id"), "leftouter")
        leftOuterJoinDataFrame
    }

    def innerJoinCustomersProducts(customers: DataFrame, products: DataFrame): DataFrame = {
        val innerJoinDataFrame = customers
            .join(products, col("customer_identifier") === col("customer_id"), "inner")
        innerJoinDataFrame
    }

    def JoinWithFilterInOrbitalPeriod(characters: DataFrame, planets: DataFrame): DataFrame = {
        val planetVisitedWithOrbitalPeriodLessThan500 = characters
            .join(planets, col("planet_visited_id").alias("planet_visited") === col("planet_id"), "inner")
            .filter(col("orbital_period") < 500)
            .select(
                col("name").as("character_name"),
                col("birth_year").as("character_birth_year"),
                col("p_name").as("planet_visited"),
                col("rotation_period").as("planet_rotation"),
                col("orbital_period").as("planet_orbital"),
                col("diameter").as("planet_diameter"),
                col("climate").as("planet_climate"),
                col("terrain").as("planet_terrain")
            )
        planetVisitedWithOrbitalPeriodLessThan500
    }
}
