/* fb_csv.scala
This script should load FB prices from Yahoo.

Demo:
spark-shell -i fb_csv.scala
*/

// I should get prices:
import sys.process._
"/usr/bin/curl -o /tmp/fb.csv http://ichart.finance.yahoo.com/table.csv?s=FB"!

import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)

val fb_df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("/tmp/fb.csv")

fb_df.head(9)

