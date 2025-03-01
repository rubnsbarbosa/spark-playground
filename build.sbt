lazy val root = (project in file(".")).settings(
    version := "1.0.0-SNAPSHOT",
    name := "spark-playground",
    scalaVersion := "2.13.12",
    organization := "com.playground",
    mainClass := Some("com.playground.PlaygroundMain"),
    scalacOptions += "-Ymacro-annotations"
)

val versions = new Object {
    val sparkVersion = "3.5.3"
}

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % versions.sparkVersion
)
