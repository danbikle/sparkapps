/* ~/sparkapps/dollar_df.scala
This script should demo $-notation with DataFrame syntax.

Demo:
spark-shell -i dollar_df.scala
*/

import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
// This import is needed to use the $-notation
import spark.implicits._

// I should get prices:
import sys.process._
"/usr/bin/curl -o /tmp/gspc.csv http://ichart.finance.yahoo.com/table.csv?s=%5EGSPC"!

val sqlContext = new SQLContext(sc)

val gspc10_df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("/tmp/gspc.csv")

// I should Register the DataFrame as a SQL temporary view
gspc10_df.createOrReplaceTempView("gspc10_table")

var sql_str = "SELECT Date, Close "
    sql_str=sql_str++" FROM gspc10_table WHERE Date BETWEEN '2016-01-01'AND'2016-01-09'"

val gspc11_df = spark.sql(sql_str)

gspc11_df.select($"Date",$"Close",$"Close"+1.1).show()

/*
+--------------------+-----------+------------------+
|                Date|      Close|     (Close + 1.1)|
+--------------------+-----------+------------------+
|2016-01-08 00:00:...|1922.030029|       1923.130029|
|2016-01-07 00:00:...|1943.089966|       1944.189966|
|2016-01-06 00:00:...| 1990.26001|1991.3600099999999|
|2016-01-05 00:00:...|2016.709961|       2017.809961|
|2016-01-04 00:00:...|2012.660034|       2013.760034|
+--------------------+-----------+------------------+
*/
gspc11_df.select($"Date",$"Close",$"Close"+1.1).filter($"Close" > 2000.0).show()
