val CirceVersion = "0.13.0"
val MunitVersion = "0.7.20"
val SparkVersion = "3.1.1"
val CatsVersion = "2.0.0"

lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    organization := "com.example",
    name := "bdm",
    version := "0.0.1",
    scalaVersion := "2.12.10",

    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core",
    ).map(_ % CatsVersion) ++ Seq(
      "io.circe" %% "circe-core",
      "io.circe" %% "circe-generic",
      "io.circe" %% "circe-parser"
    ).map(_ % CirceVersion) ++ Seq(
      "org.apache.spark" %% "spark-core",
      "org.apache.spark" %% "spark-sql",
    ).map(_ % SparkVersion) ++ Seq(
      "org.scalameta" %% "munit" % MunitVersion % Test
    ) ++ Seq(
      "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
    ),

    resolvers ++= Seq(
        "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
        "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
        "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
        Resolver.sonatypeRepo("public")
    ),

    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
    addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full),

    testFrameworks += new TestFramework("munit.Framework")
  )
