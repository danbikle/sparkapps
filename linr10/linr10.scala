/* ~/sparkapps/linr10/linr10.scala

This script should download some data.
It should generate a dependent response variable named pctlead.
It should generate independent features from slopes of moving averages of prices.
It should create a Linear Regression model from the features.

Demo:
spark-shell -i linr10.scala
*/
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
// This import is needed to use the $-notation
import spark.implicits._

// I should get prices:
import sys.process._
"/usr/bin/curl -o /tmp/gspc.csv http://ichart.finance.yahoo.com/table.csv?s=%5EGSPC"!

val sqlContext = new SQLContext(sc)

val gspc10_df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("/tmp/gspc.csv")

// I should Register the DataFrame as a SQL temporary view
gspc10_df.createOrReplaceTempView("gspc10_table")

var sql_str = "SELECT Date, Close, LEAD(Close,1)OVER(ORDER BY Date) AS leadp "
    sql_str=sql_str++" FROM gspc10_table ORDER BY Date"

val gspc11_df = spark.sql(sql_str)

//"SELECT Date,Close, AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mvgavg3day FROM gspc2_table ORDER BY Date")

gspc11_df.createOrReplaceTempView("gspc11_table")

var sql_str = "SELECT Date, Close, 100.0*(leadp - Close)/Close AS pctlead "
    sql_str=sql_str++" FROM gspc11_table ORDER BY Date"

val gspc12_df = spark.sql(sql_str)

gspc12_df.createOrReplaceTempView("gspc12_table")
var sql_str = "SELECT Date, Close, pctlead"
sql_str=sql_str++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mavg2"
sql_str=sql_str++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS mavg3"
sql_str=sql_str++" FROM gspc12_table ORDER BY Date"
val gspc13_df = spark.sql(sql_str)

gspc13_df.createOrReplaceTempView("gspc13_table")
var sql_str = "SELECT Date, Close, pctlead"
sql_str=sql_str++",(mavg2-LAG(mavg2,1)OVER(ORDER BY Date))/mavg2 AS slp2 "
sql_str=sql_str++",(mavg3-LAG(mavg3,1)OVER(ORDER BY Date))/mavg3 AS slp3 "
sql_str=sql_str++" FROM gspc13_table ORDER BY Date"
val gspc14_df = spark.sql(sql_str)

// I should get test data:
gspc14_df.createOrReplaceTempView("gspc14_table")
var sql_str = "SELECT Date, Close, pctlead,slp2,slp3 "
sql_str=sql_str++" FROM gspc14_table WHERE Date > '2016-01-01' ORDER BY Date"
val gspc15_df = spark.sql(sql_str)

// I should get training data:
gspc15_df.createOrReplaceTempView("gspc15_table")
var sql_str = "SELECT Date, Close, pctlead,slp2,slp3 "
sql_str=sql_str++" FROM gspc14_table WHERE Date BETWEEN '1986-01-01' AND '2016-01-01' ORDER BY Date"
val gspc16_df = spark.sql(sql_str)

// I should get jnk data:
gspc14_df.createOrReplaceTempView("gspc14_table")
var sql_str = "SELECT Date, Close, pctlead,slp2,slp3 "
sql_str=sql_str++" FROM gspc14_table WHERE Date > '2016-09-10' ORDER BY Date"
val gspc17_df = spark.sql(sql_str)

// http://stackoverflow.com/questions/39651170/in-apache-spark-scala-how-to-fill-vectors-dense-in-dataframe-from-csv
// http://spark.apache.org/docs/latest/ml-pipeline.html

gspc17_df.show

val gspc18_df = gspc17_df.select("pctlead","slp2","slp3").withColumn("yval", col("pctlead"))

gspc18_df.show
/*
scala> gspc18_df.show
+--------------------+--------------------+--------------------+--------------------+
|             pctlead|                slp2|                slp3|                yval|
+--------------------+--------------------+--------------------+--------------------+
| -1.4830674013266898|-0.00419283294043...|-0.00317066765726...| -1.4830674013266898|
|-0.05876766500768526|-0.00846291365452...|-0.00688059582892...|-0.05876766500768526|
|  1.0109273250546658|-3.18167976204166...|-0.00650261932618...|  1.0109273250546658|
| -0.5736774814255415|0.005873373804173195|0.004411094700847...| -0.5736774814255415|
|                null|0.003832431877423...|0.002957844240723...|                null|
+--------------------+--------------------+--------------------+--------------------+
*/

val sqlfnc = udf((arg1: Double) => Seq(Vectors.dense(arg1)))
val gspc19_df = gspc18_df.select("pctlead","slp2","slp3","yval").withColumn("features", sqlfnc(col("slp2")))

gspc19_df.show

/*
scala> gspc19_df.show
+--------------------+--------------------+--------------------+--------------------+--------------------+
|             pctlead|                slp2|                slp3|                yval|            features|
+--------------------+--------------------+--------------------+--------------------+--------------------+
| -1.4830674013266898|-0.00419283294043...|-0.00317066765726...| -1.4830674013266898|[[-0.004192832940...|
|-0.05876766500768526|-0.00846291365452...|-0.00688059582892...|-0.05876766500768526|[[-0.008462913654...|
|  0.6499784681166678|0.003719373500849...|0.001848247406140...|  0.6499784681166678|[[0.0037193735008...|
| -0.5736774814255415|0.005873373804173195|0.004411094700847...| -0.5736774814255415|[[0.0058733738041...|
|                null|0.003832431877423...|0.002957844240723...|                null|[[0.0038324318774...|
+--------------------+--------------------+--------------------+--------------------+--------------------+
*/

val sqlfnc = udf((arg1: Double,arg2: Double) => Seq(Vectors.dense(arg1,arg2)))
val gspc19_df = gspc18_df.select("pctlead","slp2","slp3","yval").withColumn("features", sqlfnc(col("slp2"),col("slp3")))

gspc19_df.show

/*
scala> gspc19_df.show
+--------------------+--------------------+--------------------+--------------------+--------------------+
|             pctlead|                slp2|                slp3|                yval|            features|
+--------------------+--------------------+--------------------+--------------------+--------------------+
| -1.4830674013266898|-0.00419283294043...|-0.00317066765726...| -1.4830674013266898|[[-0.004192832940...|
|-0.05876766500768526|-0.00846291365452...|-0.00688059582892...|-0.05876766500768526|[[-0.008462913654...|
|  1.0109273250546658|-3.18167976204166...|-0.00650261932618...|  1.0109273250546658|[[-3.181679762041...|
| -0.3772294907126729|-0.00184061513706...|0.002272432092401652| -0.3772294907126729|[[-0.001840615137...|
|                null|0.003832431877423...|0.002957844240723...|                null|[[0.0038324318774...|
+--------------------+--------------------+--------------------+--------------------+--------------------+


scala> gspc19_df.select("features").collect.map{x => x}
res228: Array[org.apache.spark.sql.Row] =
Array
 ([WrappedArray([-0.004192832940431825,-0.003170667657263393])]
, [WrappedArray([-0.008462913654529357,-0.006880595828929472])]
, [WrappedArray([-3.1816797620416693E-4,-0.006502619326182358])]
, [WrappedArray([-0.0018406151370642958,0.002272432092401652])]
, [WrappedArray([0.0018932520885701712,-0.002328099096809459])]
, [WrappedArray([0.0020776614757624656,0.001414999212578387])]
, [WrappedArray([-0.001168581055030093,0.0016333333239894343])]
, [WrappedArray([0.0037193735008497495,0.0018482474061401757])]
, [WrappedArray([0.005873373804173195,0.0044110947008473055])]
, [WrappedArray([0.0038324318774239446,0.0029578442407239463])])

*/

/* Still under construction...
It is not clear to me how to proceed from here.
I will work with logistic regression for awhile because I see a nice demo I can learn from.
*/
