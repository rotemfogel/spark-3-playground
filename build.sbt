name := "spark-3-playground"

version := "0.1"

scalaVersion := "2.12.15"

// @formatter:off
libraryDependencies ++= {
  val sparkVersion = "3.2.1"
  val deequVersion = "2.0.1-spark-3.2"
  val log4jVersion = "2.17.2"
  val yauaaVersion = "7.2.0"
  Seq(
    "org.apache.spark"          %% "spark-mllib"   % sparkVersion, // % Provided,
    "org.apache.hadoop"          % "hadoop-aws"    % "3.3.2"      % Provided,
    "com.github.scopt"          %% "scopt"         % "4.0.1",
    "com.typesafe"               % "config"        % "1.4.2",
    "com.amazon.deequ"           % "deequ"         % deequVersion,
    "nl.basjes.parse.useragent"  % "yauaa"         % yauaaVersion,
    "org.apache.logging.log4j"   % "log4j-core"    % log4jVersion,
    "org.scalatest"             %% "scalatest"     % "3.2.12"     % Test
  )
}
// @formatter:on

scalacOptions ++= Seq(
  "-encoding",
  "UTF-8",
  "-target:jvm-1.8",
  "-deprecation",
  "-feature",
  "-unchecked",
  // Fail the compilation if there are any warnings
  "-Xfatal-warnings",
  "-Xfuture",
  "-Xlint",
  "-Yrangepos",
  "-Ywarn-dead-code",
  "-Ywarn-nullary-unit",
  "-Ywarn-unused-import"
)
