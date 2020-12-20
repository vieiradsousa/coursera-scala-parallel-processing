name := "coursera-scala-parallel-processing"

version := "0.1"

scalaVersion := "2.13.4"

scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")
libraryDependencies ++= Seq(
  "com.storm-enroute" %% "scalameter-core" % "0.19",
  "org.scala-lang.modules" %% "scala-parallel-collections" % "0.2.0",
  "com.novocode" % "junit-interface" % "0.11" % Test
)

testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "-s")
