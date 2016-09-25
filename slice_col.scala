/* ~/sparkapps/slice_col.scala

Demo:
spark-shell -i slice_col.scala
*/

import org.apache.spark.sql.SQLContext

// I should get prices:
import sys.process._
"/usr/bin/curl -o /tmp/gspc.csv http://ichart.finance.yahoo.com/table.csv?s=%5EGSPC"!

val sqlContext = new SQLContext(sc)

val gspc10_df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("/tmp/gspc.csv")

val narrow_df = gspc10_df.select("Date", "Close")
narrow_df.show()
/*
+--------------------+-----------+
|                Date|      Close|
+--------------------+-----------+
|2016-09-23 00:00:...|2164.689941|
|2016-09-22 00:00:...|2177.179932|
|2016-09-21 00:00:...|2163.120117|
|2016-09-20 00:00:...| 2139.76001|
|2016-09-19 00:00:...|2139.120117|
|2016-09-16 00:00:...|2139.159912|
|2016-09-15 00:00:...| 2147.26001|
|2016-09-14 00:00:...| 2125.77002|
|2016-09-13 00:00:...| 2127.02002|
|2016-09-12 00:00:...|2159.040039|
|2016-09-09 00:00:...|2127.810059|
|2016-09-08 00:00:...|2181.300049|
|2016-09-07 00:00:...|2186.159912|
|2016-09-06 00:00:...| 2186.47998|
|2016-09-02 00:00:...| 2179.97998|
|2016-09-01 00:00:...|2170.860107|
|2016-08-31 00:00:...|2170.949951|
|2016-08-30 00:00:...|2176.120117|
|2016-08-29 00:00:...|2180.379883|
|2016-08-26 00:00:...|2169.040039|
+--------------------+-----------+
only showing top 20 rows


scala>
*/