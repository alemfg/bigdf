name := "bigdf"

version := "0.2"

scalaVersion := "2.10.3"

scalacOptions += "-feature"

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "org.scalanlp" % "breeze-natives_2.10" % "0.7",
  "org.apache.commons" % "commons-math3" % "3.0",
  "commons-io" % "commons-io" % "2.4",
  "joda-time" % "joda-time" % "2.0",
  "org.joda" % "joda-convert" % "1.3.1",
  "com.quantifind" %% "sumac" % "0.3.0",
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
  "com.univocity" % "univocity-parsers" % "1.3.1",
  "org.apache.spark" % "spark-assembly" % "1.3.0-SNAPSHOT-hadoop2.5.0-cdh5.2.1"
)

resolvers ++= Seq(
  // other resolvers here
  // if you want to use snapshot builds (currently 0.8-SNAPSHOT), use this.
  //"Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/releases/",
  "Local Maven Repository" at "file:///" + Path.userHome + ".m2/repository"
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


