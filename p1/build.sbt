val CirceVersion = "0.13.0"
val MunitVersion = "0.7.20"

lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    organization := "com.example",
    name := "p1",
    version := "0.0.1",
    scalaVersion := "2.13.4",
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % MunitVersion % Test
    ),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
    testFrameworks += new TestFramework("munit.Framework")
  )
