package com.playground.models

case class Customer(
    customer_identifier: Int,
    name: String,
    age: Int
)

case class Products(
    product_id: Int,
    customer_id: Int,
    name: String
)

case class StarWarsCharacters(
    id: Int,
    name: String,
    height: String,
    mass: String,
    birth_year: String,
    gender: String,
    url: String
)

case class StarWarsStarships(
    name: String,
    model: String,
    manufacturer: String,
    length: String,
    max_atmosphere_speed: String,
    passengers: String,
    cargo_capacity: String
)

case class StarWarsSpecies(
    name: String,
    classification: String,
    designation: String,
    average_height: String,
    average_lifespan: String,
    language: String
)
