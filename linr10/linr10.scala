/* ~/sparkapps/linr10/linr10.scala

This script should download some data.
It should generate a dependent response variable named pctlead.
It should generate independent features from slopes of moving averages of prices.
It should create a Linear Regression model from the features.

Demo:
spark-shell -i linr10.scala
*/

// I should get prices:
import sys.process._
"/usr/bin/curl -o /tmp/gspc.csv http://ichart.finance.yahoo.com/table.csv?s=%5EGSPC"!

import org.apache.spark.sql.SQLContext

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

gspc17_df.collect().map {row => Array(row(0),row(1))}

