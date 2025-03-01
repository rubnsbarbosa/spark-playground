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
}
