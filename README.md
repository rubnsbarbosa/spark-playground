# Spark Playground

This playground shows you how to work with Apache Spark running on localhost

```scala
val spark = SparkSession.builder()
    .appName("SparkPlayground")
    .master("local[*]")
    .getOrCreate()
```

## Prerequisites
* java version >= 8
* sbt version == 1.10.7
* a modern ide (intellij) 

## How to run

```bash
sbt clean compile
sbt "run PlaygroundMain"
```

## Reference

Playground inspired by [Apache Spark](http://spark.apache.org/).

## Additional Links

These additional references from [SWAPI](https://swapi.dev/api/) helped to create Spark DataFrames

```json
{
    "people": "https://swapi.dev/api/people/", 
    "planets": "https://swapi.dev/api/planets/", 
    "films": "https://swapi.dev/api/films/", 
    "species": "https://swapi.dev/api/species/", 
    "vehicles": "https://swapi.dev/api/vehicles/", 
    "starships": "https://swapi.dev/api/starships/"
}
```
