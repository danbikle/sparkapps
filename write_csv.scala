/* ~/sparkapps/write_csv.scala

Demo:
spark-shell -i write_csv.scala
*/

import org.apache.spark.sql.SQLContext

// I should get prices:
import sys.process._
"/usr/bin/curl -o /tmp/gspc.csv http://ichart.finance.yahoo.com/table.csv?s=%5EGSPC"!

val sqlContext = new SQLContext(sc)

val gspc10_df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("/tmp/gspc.csv")

val narrow_df = gspc10_df.select("Date", "Close")
// I should write a CSV file inside folder: /tmp/gspc10csv_dir
narrow_df.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save("/tmp/gspc10csv_dir")
