val scala3Version = "3.3.1"

lazy val root = project
  .in(file("."))
  .settings(
    name := "scala3-macros",
    version := "0.1.0",

    // Additional check useful during development
    scalacOptions ++= Seq(
      "-Xcheck-macros"
    ),

    scalaVersion := scala3Version,
  )

libraryDependencies += "com.ibm.db2" % "jcc" % "11.5.7.0"
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.4.0"

