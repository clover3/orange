lazy val commonSettings = Seq(
  version := "0.1.0",
  scalaVersion := "2.11.7"
)

lazy val master = (project in file("master")).
  settings(commonSettings: _*).
  settings(
    name := "orangeMaster"
  )


lazy val slave = (project in file("slave")).
  settings(commonSettings: _*).
  settings(
    name := "orangeSlave"
  )

