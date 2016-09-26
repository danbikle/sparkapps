/* ~/sparkapps/logr10/logr10.scala
This script should download some data.
It should generate a label which I assume to be dependent on price calculations.
It should generate independent features from slopes of moving averages of prices.
It should create a Logistic Regression model from the features.
Demo:
spark-shell -i logr10.scala
*/

import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._


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

gspc11_df.createOrReplaceTempView("gspc11_table")

var sql_str = "SELECT Date, Close, 100.0*(leadp - Close)/Close AS pctlead "
sql_str=sql_str++" FROM gspc11_table ORDER BY Date"

val gspc12_df = spark.sql(sql_str)

gspc12_df.createOrReplaceTempView("gspc12_table")
var sql_str = "SELECT Date, Close, pctlead"
sql_str=sql_str++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mavg2"
sql_str=sql_str++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS mavg3"
sql_str=sql_str++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS mavg4"
sql_str=sql_str++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS mavg5"
sql_str=sql_str++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS mavg6"
sql_str=sql_str++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS mavg7"
sql_str=sql_str++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) AS mavg8"
sql_str=sql_str++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS mavg9"
sql_str=sql_str++" FROM gspc12_table ORDER BY Date"
val gspc13_df = spark.sql(sql_str)

gspc13_df.createOrReplaceTempView("gspc13_table")
var sql_str = "SELECT Date, Close, pctlead"
sql_str=sql_str++",(mavg2-LAG(mavg2,1)OVER(ORDER BY Date))/mavg2 AS slp2 "
sql_str=sql_str++",(mavg3-LAG(mavg3,1)OVER(ORDER BY Date))/mavg3 AS slp3 "
sql_str=sql_str++",(mavg3-LAG(mavg4,1)OVER(ORDER BY Date))/mavg3 AS slp4 "
sql_str=sql_str++",(mavg3-LAG(mavg5,1)OVER(ORDER BY Date))/mavg3 AS slp5 "
sql_str=sql_str++",(mavg3-LAG(mavg6,1)OVER(ORDER BY Date))/mavg3 AS slp6 "
sql_str=sql_str++",(mavg3-LAG(mavg7,1)OVER(ORDER BY Date))/mavg3 AS slp7 "
sql_str=sql_str++",(mavg3-LAG(mavg8,1)OVER(ORDER BY Date))/mavg3 AS slp8 "
sql_str=sql_str++",(mavg3-LAG(mavg9,1)OVER(ORDER BY Date))/mavg3 AS slp9 "
sql_str=sql_str++" FROM gspc13_table ORDER BY Date"
val gspc14_df = spark.sql(sql_str)

// I should compute label from pctlead:
val pctlead2label = udf((pctlead:Double)=> {if (pctlead>0.0) 1.0 else 0.0}) 

val gspc17_df = gspc14_df.withColumn("label",pctlead2label(col("pctlead")))
gspc17_df.select("pctlead","label").show
/*
I should see something like this:
+--------------------+-----+
|             pctlead|label|
+--------------------+-----+
|  1.1404561824729968|  1.0|
|  0.4747774480712065|  1.0|
| 0.29533372711164035|  1.0|
|   0.588928150765594|  1.0|
| -0.2927341920374689|  0.0|
|  0.3523135436104863|  1.0|
*/

gspc17_df.createOrReplaceTempView("gspc17_table")

// I should copy slp-values into Vectors.dense():
val fill_vec = udf((slp2:Double,slp3:Double,slp4:Double,slp5:Double,slp6:Double,slp7:Double,slp8:Double,slp9:Double)=> {Vectors.dense(slp2,slp3,slp4,slp5,slp6,slp7,slp8,slp9)})

val gspc19_df = gspc17_df.withColumn("features",fill_vec(col("slp2"),col("slp3"),col("slp4"),col("slp5"),col("slp6"),col("slp7"),col("slp8"),col("slp9")))
gspc19_df.select("pctlead","label","features").show

/*
I should see something like this:
+--------------------+-----+--------------------+
|             pctlead|label|            features|
+--------------------+-----+--------------------+
|  1.1404561824729968|  1.0|                null|
|  0.4747774480712065|  1.0|[0.00566994926887...|
| 0.29533372711164035|  1.0|[0.00346946867565...|
|   0.588928150765594|  1.0|[0.00630417651694...|
*/

// features are hard to see. This should help:
gspc19_df.select("features").collect().map{r =>r}.slice(0,2)
/*
I should see something like this:
res66: Array[org.apache.spark.sql.Row] = Array([null], [
[0.005669949268875106
,0.005669949268875106
,0.005669949268875106
,0.005669949268875106
,0.005669949268875106
,0.005669949268875106
,0.005669949268875106
,0.005669949268875106]])
*/

/* I should mimic this structure.
ref:
http://spark.apache.org/docs/latest/ml-pipeline.html#example-estimator-transformer-and-param
*/
val my_df = spark.createDataFrame(Seq((1.0, Vectors.dense(0.0, 1.1, 0.1)),(0.0, Vectors.dense(2.0, 1.0, -1.0)))).toDF("label", "features")
my_df.show
/*
I should see something like this:
+-----+--------------+
|label|      features|
+-----+--------------+
|  1.0| [0.0,1.1,0.1]|
|  0.0|[2.0,1.0,-1.0]|
+-----+--------------+
*/
