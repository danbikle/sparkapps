/* ~/sparkapps/logr10/logr11.scala
This script should download some data.
It should generate a label which I assume to be dependent on price calculations.
A label should classify an observation as down or up. Down is 0.0, up is 1.0.
It should generate independent features from slopes of moving averages of prices.
It should create a Logistic Regression model from many years of features.
This script is a less-verbose enhancement of logr10.scala.
Demo:
spark-shell -i logr11.scala
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

var sql_s="SELECT Date,Close,LEAD(Close,1)OVER(ORDER BY Date) leadp FROM gspc10_table ORDER BY Date"

val gspc11_df = spark.sql(sql_s)

gspc11_df.createOrReplaceTempView("gspc11_table")

var sql_str = "SELECT Date,Close,100*(leadp-Close)/Close pctlead FROM gspc11_table ORDER BY Date"

val gspc12_df = spark.sql(sql_str)

gspc12_df.createOrReplaceTempView("gspc12_table")
