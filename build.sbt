
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"

libraryDependencies += "io.netty" % "netty-all" % "4.0.4.Final"

unmanagedBase <<= baseDirectory { base => base / "lib" }

lazy val commonSettings = Seq(
  version := "0.1.0",
  scalaVersion := "2.11.7"
    , libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"
    , libraryDependencies += "org.scala-lang.modules" %% "scala-async" % "0.9.2"
    , libraryDependencies += "io.netty" % "netty-all" % "4.0.4.Final"
    , libraryDependencies += "commons-logging" % "commons-logging" % "1.2"
    , fork in run := true
    , connectInput in run := true
)

lazy val common = (project in file("common")).
  settings(commonSettings: _*).
  settings(
    name := "orangeCommon"
  )

lazy val master = (project in file("master")).
  settings(commonSettings: _*).
  settings(
    name := "orangeMaster"
    , mainClass in Compile := Some("master.Main")
  ).dependsOn(common)


lazy val slave = (project in file("slave")).
  settings(commonSettings: _*).
  settings(
    name := "orangeSlave"
    , mainClass in Compile := Some("slave.Main")
  ).dependsOn(common)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}
