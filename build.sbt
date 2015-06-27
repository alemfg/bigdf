name := "bigdf"

version := "0.3"

scalaVersion := "2.10.4"

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
  "com.univocity" % "univocity-parsers" % "1.5.1",
  "com.databricks" % "spark-csv" % "1.1.0"
)

//
//libraryDependencies ++=  Seq(
//  ("org.apache.hadoop" % "hadoop-client" % "2.5.0-cdh5.2.1").
//    exclude("org.slf4j", "slf4j-api").
//    exclude("javax.")
//)


libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-sql" % "1.4.0").
//    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-logging", "commons-logging").
    exclude("org.slf4j", "slf4j-api").
    exclude("org.apache.hadoop", "hadoop-yarn-api").
    exclude("org.apache.hadoop", "hadoop-yarn-common").
    exclude("com.esotericsoftware.minlog", "minlog")
)

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

resolvers ++= Seq(
  // other resolvers here
  // if you want to use snapshot builds (currently 0.8-SNAPSHOT), use this.
  //"Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/releases/"
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


