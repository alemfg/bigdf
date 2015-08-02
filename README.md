### Introduction
DataFrames are useful constructs for data scientists, popularized by [R] and [pandas].

[pandas]:http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html
[R]:http://www.r-tutor.com/r-introduction/data-frame
[https://github.com/AyasdiOpenSource/spark]:https://github.com/AyasdiOpenSource/spark
Implementations of these packages are not natively distributed - their DataFrames struggle with big data. bigdf is a dataframe on top of Apache Spark. It is written as an internal Scala DSL with the look and feel of Pandas to make it easy for those familiar with Pandas to use it.

A gentle introduction is available at:
> http://www.slideshare.net/codeninja4086/df-38948475?utm_source=slideshow03&utm_medium=ssemail&utm_campaign=iupload_share_slideshow


### How to get it
- Choose a repos directory (make one if it doesn't exist)
```sh
export $REPOS = ~/Documents/repos
```
- Install maven, scala and sbt (brew instructions below)
```sh
$ brew install maven
$ brew install scala
$ brew install sbt
```
- clone the bigdf repo
```sh
$ cd $REPOS
$ git clone git@bitbucket.org:ayasdi/bigdf.git
```
- sbt update, package, and test(optional) in the bigdf directory.
```sh
$ cd bigdf
$ sbt update
$ sbt package
$ sbt assembly
$ sbt test #this might fail, but will pass from eclipse and intellij
```
- Start a shell and begin playing. You need spark 1.3.1
```sh
$ cd $REPOS
$ ./spark/bin/spark-shell --jars ./bigdf/target/scala-2.10/bigdf-assembly-*.jar 
```
- https://github.com/AyasdiOpenSource/bigdf/wiki/ for examples
- Look at DFTest.scala for usage [a simple example is shown below]

### Use with ipython notebook
```sh
$cd $REPOS
$git clone git@github.com:mattpap/IScala.git
$cd IScala
$sbt
>++ 2.10.4
>assembly
>^D
```

```sh
$ipython profile create scala
```
Add the following line to the three files ~/.ipython/profile_scala/ipython*_config.py

```sh
c.KernelManager.kernel_cmd = ["java", "-jar", "$ISCALA_PATH/lib/IScala.jar", "--connection-file", "{connection_file}", "--parent"]"
```

To start notebook
```sh
$cd <my notebooks directory>
$ipython notebook --profile scala
```

To install spark and bigdf to your local mvn repos
```sh
$mvn install:install-file -Dfile=$REPOS/bigdf-transforms/lib/spark-assembly-1.2.0-SNAPSHOT-hadoop1.0.4.jar -DgroupId=org.apache -DartifactId=spark-assembly_2.10 -Dversion=1.2.0-SNAPSHOT-hadoop1.0.4 -Dpackaging=jar
$ mvn install:install-file -Dfile=$REPOS/bigdf-transforms/bigdf/target/scala-2.10/bigdf-assembly-0.2.jar -DgroupId=com.ayasdi -DartifactId=bigdf-assembly_2.10 -Dversion=0.2 -Dpackaging=jar
```

To start using bigdf in a notebook
```sh
%libraryDependencies += "org.apache" %% "spark-assembly" % "1.2.0-SNAPSHOT-hadoop1.0.4"
%libraryDependencies += "com.ayasdi" %% "bigdf-assembly" % "0.2"
%resolvers += "Local Maven Repository" at "file:///Users/mohit/.m2/repository"
%update
```

```sh
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import com.ayasdi.bigdf._

val sc = new SparkContext("local[*]", "bigdf-intro")
val df = DF(sc, "/Users/mohit/Downloads/faredata2013/trip_fare_1.csv", ',', true)
```
