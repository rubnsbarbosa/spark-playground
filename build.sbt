lazy val root = (project in file(".")).settings(
    version := "1.0.0-SNAPSHOT",
    name := "spark-playground",
    scalaVersion := "2.13.12",
    organization := "com.data.eng",
    mainClass := Some("com.data.eng.KafkaBatchMain"),
    scalacOptions += "-Ymacro-annotations"
)

val versions = new Object {
    val sparkVersion = "3.5.3"
}

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % versions.sparkVersion
)
