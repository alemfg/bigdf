name := "bigdf"

version := "2.0-SNAPSHOT"

scalaVersion := "2.10.4"

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "com.univocity" % "univocity-parsers" % "1.5.1"
)

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % Test

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.0" % Provided

resolvers ++= Seq(
  // other resolvers here
  // if you want to use snapshot builds (currently 0.8-SNAPSHOT), use this.
  //"Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/releases/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

pomExtra := (
  <url>https://github.com/AyasdiOpenSource/bigdf</url>
    <licenses>
      <license>
        <name>Apache 2.0</name>
        <url>http://www.apache.org/licenses/</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>https://github.com/AyasdiOpenSource/bigdf.git</url>
      <connection>scm:git:git@github.com/AyasdiOpenSource/bigdf.git</connection>
    </scm>
    <developers>
      <developer>
        <id>mohitjaggi</id>
        <name>Mohit Jaggi</name>
        <url>http://ayasdi.com</url>
      </developer>
    </developers>)

lazy val bigdf = project.in(file("."))
  .dependsOn(sparkCsv)

lazy val sparkCsv = project in file("spark-csv")

