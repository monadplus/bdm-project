val ScalaVersion = "2.12.13"
val CirceVersion = "0.13.0"
val MunitVersion = "0.7.20"
val SparkVersion = "3.1.1"
val CatsVersion = "2.6.0"
val BetterMonadicForVersion = "0.3.1"
val KindProjectorVersion = "0.13.0"

lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    organization := "com.example",
    name := "bdm",
    version := "0.0.1",
    scalaVersion := ScalaVersion,

    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core",
    ).map(_ % CatsVersion) ++ Seq(
      "io.circe" %% "circe-core",
      "io.circe" %% "circe-generic",
      "io.circe" %% "circe-parser"
    ).map(_ % CirceVersion) ++ Seq(
      "spark-core",
      "spark-sql",
      "spark-mllib",
      "spark-streaming",
    ).map("org.apache.spark" %% _ % SparkVersion) ++ Seq(
      "org.scalameta" %% "munit" % MunitVersion % Test
    ),

    resolvers ++= Seq(
        "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
        "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
        "Second Typesafe repo" at "https://repo.typesafe.com/typesafe/maven-releases/",
        Resolver.sonatypeRepo("public")
    ),

    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % BetterMonadicForVersion),
    addCompilerPlugin("org.typelevel" %% "kind-projector" % KindProjectorVersion cross CrossVersion.full),

    testFrameworks += new TestFramework("munit.Framework")
  )
