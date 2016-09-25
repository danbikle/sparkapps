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

val fb_df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("/tmp/gspc.csv")

fb_df.head(9)


