val sparkVersion = "3.4.0"
// https://github.com/combust/mleap
val mleapVersion = "0.23.0"

val settings: Seq[Def.Setting[_]] = Seq(
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.12.13",
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
)

lazy val root = (project in file("."))
  .settings(
    name := "mleap-1-airbnb"
  )
  .aggregate(
    model,
    server
  )

lazy val model = (project in file("model"))
  .settings(settings)
  .settings(libraryDependencies ++= modelDependencies)

lazy val server = (project in file("server"))
  .settings(settings)
  .settings(libraryDependencies ++= serverDependencies)

lazy val modelDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion,
  "ml.combust.mleap" %% "mleap-runtime" % mleapVersion,
  "ml.combust.mleap" %% "mleap-spark" % mleapVersion,
  "org.scalanlp" %% "breeze-natives" % "2.1.0"
)

lazy val serverDependencies = Seq(
  "ml.combust.mleap" %% "mleap-runtime" % mleapVersion,
  "ml.combust.mleap" %% "mleap-spark" % mleapVersion,
)