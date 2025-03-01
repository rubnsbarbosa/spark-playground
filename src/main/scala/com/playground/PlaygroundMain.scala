package com.playground

import com.playground.sandbox.Sandbox
import org.apache.spark.sql.SparkSession

object PlaygroundMain extends Serializable {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName("SparkPlayground")
            .master("local[*]")
            .getOrCreate()

        new Sandbox(spark).run()
        spark.stop()
    }
}
