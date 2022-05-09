name := "spark-3-playground"

version := "0.1"

scalaVersion := "2.12.15"

// @formatter:off
libraryDependencies ++= {
  val sparkVersion = "3.1.2"
  val deequVersion = "2.0.0-spark-3.1"
  val log4jVersion = "2.17.1"
  val yauaaVersion = "6.8"
  Seq(
    "org.apache.spark"          %% "spark-mllib"   % sparkVersion, // % Provided,
    "org.apache.hadoop"          % "hadoop-aws"    % "3.3.1"      % Provided,
    "com.github.scopt"          %% "scopt"         % "4.0.1",
    "com.typesafe"               % "config"        % "1.4.1",
    "com.amazon.deequ"           % "deequ"         % deequVersion,
    "nl.basjes.parse.useragent"  % "yauaa"         % yauaaVersion,
    "org.apache.logging.log4j"   % "log4j-core"    % log4jVersion,
    "com.nrinaudo"              %% "kantan.csv"    % "0.6.2",
    "org.scalatest"             %% "scalatest"     % "3.0.5"      % Test
  )
}
// @formatter:on

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
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

