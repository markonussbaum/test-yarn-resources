ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"


// Yarn client app
lazy val root = (project in file("."))
  .settings(
    name := "TYR",
    libraryDependencies ++= commonDependencies,
    assembly / assemblyJarName := "TYRYarnClient-assembly.jar",
    assembly / mainClass := Some("ai.xpress.testyarnresources.TYRYarnClient"),
  )
  .aggregate(appMaster, application)

// Application master
lazy val appMaster = (project in file("ApplicationMaster"))
  .settings(
    name := "TYRAM",
    libraryDependencies ++= commonDependencies,
    assembly / assemblyJarName := "TYRApplicationMaster-assembly.jar",
    assembly / mainClass := Some("ai.xpress.testyarnresources.TYRApplicationMaster"),
  )

// The actual "Payload" application to run on the cluster
lazy val application = (project in file("Application"))
  .settings(
    name := "TYRApp",
    libraryDependencies ++= commonDependencies,
    assembly / assemblyJarName := "TYRApp-assembly.jar",
    assembly / mainClass := Some("ai.xpress.testyarnresources.TYRMain"),
  )

lazy val commonDependencies = Seq(
  /*
  "org.apache.hadoop" % "hadoop-yarn-client" % "3.2.2" % Provided,
  "org.apache.hadoop" % "hadoop-common" % "3.2.2" % Provided,
  "org.apache.hadoop" % "hadoop-yarn-api" % "3.2.2" % Provided,
  "org.apache.hadoop" % "hadoop-hdfs" % "3.2.2" % Provided,
   */
  "org.apache.hadoop" % "hadoop-yarn-client" % "3.2.2",
  "org.apache.hadoop" % "hadoop-common" % "3.2.2",
  "org.apache.hadoop" % "hadoop-yarn-api" % "3.2.2",
  "org.apache.hadoop" % "hadoop-hdfs" % "3.2.2",
)

