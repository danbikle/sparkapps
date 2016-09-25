/* ~/sparkapps/sql_lag_lead.scala
This script should demo SQL lag() and lead() syntax.

Demo:
spark-shell -i sql_lag_lead.scala
*/
import org.apache.spark.sql.SQLContext

// I should get prices:
import sys.process._
"/usr/bin/curl -o /tmp/gspc.csv http://ichart.finance.yahoo.com/table.csv?s=%5EGSPC"!

val sqlContext = new SQLContext(sc)

val gspc10_df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("/tmp/gspc.csv")

// I should Register the DataFrame as a SQL temporary view
gspc10_df.createOrReplaceTempView("gspc10_table")

var sql_str = "SELECT Date, Close                                "
    sql_str=sql_str++",LAG( Close,1)OVER(ORDER BY Date) AS lag1  "
    sql_str=sql_str++",LEAD(Close,1)OVER(ORDER BY Date) AS lead1 "
    sql_str=sql_str++" FROM gspc10_table ORDER BY Date           "

val gspc11_df = spark.sql(sql_str)
gspc11_df.show()
