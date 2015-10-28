


libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"

unmanagedBase <<= baseDirectory { base => base / "lib" }

lazy val commonSettings = Seq(
  version := "0.1.0",
  scalaVersion := "2.11.7"
    , libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"
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
  ).dependsOn(common)


lazy val slave = (project in file("slave")).
  settings(commonSettings: _*).
  settings(
    name := "orangeSlave"
  ).dependsOn(common)


