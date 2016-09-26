/* ~/sparkapps/df_doings.scala
This script should show some syntax I might want to use.
ref:
http://spark.apache.org/docs/latest/sql-programming-guide.html

Demo:
spark-shell -i df_doings.scala
*/


import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
// This import is needed to use the $-notation
import spark.implicits._

case class Person(name: String, age: Int)

val df = Seq((Person("john", 33), 5), (Person("mike", 30), 6)).toDF("person", "id")
df.show()

// I should get prices:
import sys.process._
"/usr/bin/curl -o /tmp/gspc.csv http://ichart.finance.yahoo.com/table.csv?s=%5EGSPC"!

val sqlContext = new SQLContext(sc)

val gspc10_df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("/tmp/gspc.csv")

// Print the schema in a tree format
gspc10_df.printSchema()

// filter demo:
gspc10_df.filter($"Close" > 2160.00).show()
gspc10_df.filter($"Date" >= "2016-09-22").show()

// collect()
gspc10_df.filter($"Date" > "2016-09-22").collect().foreach { println _}

// map()
gspc10_df.filter($"Date" > "2016-09-22").collect().map{ row => row }
gspc10_df.filter($"Date" > "2016-09-22").collect().map{ row => row(0) }
gspc10_df.filter($"Date" > "2016-09-22").collect().map{ row => (row(0),row(1)) }
gspc10_df.filter($"Date" > "2016-09-22").collect().map{ row => Array(row(0),row(1)) }

// Array doings
val my_a = gspc10_df.filter($"Date" > "2016-09-01").collect().map{ row => row(0) }
// In scala how to slice last element of array?
val last_a = my_a.slice(my_a.length-1,my_a.length)
val last_elm = my_a(my_a.length-1)

// Useful:
my_a.dropRight(1)
// Fail: my_a.dropLeft(1)
my_a.drop(1) // Not dropLeft()

// Take first 3:
my_a.take(3)


/*
Add a column to a DF
Ref:
http://stackoverflow.com/questions/30219592/create-new-column-with-function-in-spark-dataframe
I see that this is a conversion function:
*/

val coder1: (Int    => String) = (arg: Int)    => {if (arg < 100) "little" else "big"}
val coder2: (Float  => String) = (arg: Float)  => {if (arg < 100) "little" else "big"}
val coder3: (Double => String) = (arg: Double) => {if (arg < 100) "little" else "big"}

import org.apache.spark.sql.functions._
val sqlfunc = udf(coder3)
val code_df = gspc10_df.withColumn("Code", sqlfunc(col("Close")))
code_df.show
/*
scala> code_df.show
+--------------------+-----------+-----------+-----------+-----------+----------+-----------+----+
|                Date|       Open|       High|        Low|      Close|    Volume|  Adj Close|Code|
+--------------------+-----------+-----------+-----------+-----------+----------+-----------+----+
|2016-09-23 00:00:...|2173.290039|    2173.75|2163.969971|2164.689941|3317190000|2164.689941| big|
|2016-08-30 00:00:...|2179.449951| 2182.27002|2170.409912|2176.120117|3006800000|2176.120117| big|
|2016-08-29 00:00:...|2170.189941| 2183.47998|2170.189941|2180.379883|2654780000|2180.379883| big|
|2016-08-26 00:00:...|2175.100098|2187.939941|2160.389893|2169.040039|3342340000|2169.040039| big|
+--------------------+-----------+-----------+-----------+-----------+----------+-----------+----+
only showing top 20 rows
scala>
*/

val sqlfunc = udf((arg: Double) => {1.1})
val vec_df = gspc10_df.select("Date","Close").withColumn("col2", sqlfunc(col("Close")))
vec_df.show

/*
Works good!
scala> vec_df.show
+--------------------+-----------+----+
|                Date|      Close|col2|
+--------------------+-----------+----+
|2016-08-31 00:00:...|2170.949951| 1.1|
|2016-08-30 00:00:...|2176.120117| 1.1|
|2016-08-29 00:00:...|2180.379883| 1.1|
|2016-08-26 00:00:...|2169.040039| 1.1|
+--------------------+-----------+----+
only showing top 20 rows
*/

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row

val sqlfunc = udf((arg: Double) => Seq(Vectors.dense(arg)))
val vec_df = gspc10_df.select("Date","Close").withColumn("col2", sqlfunc(col("Close")))
vec_df.show
/*
works good:
scala> vec_df.show
+--------------------+-----------+---------------+
|                Date|      Close|           col2|
+--------------------+-----------+---------------+
|2016-09-23 00:00:...|2164.689941|[[2164.689941]]|
|2016-09-22 00:00:...|2177.179932|[[2177.179932]]|
|2016-09-21 00:00:...|2163.120117|[[2163.120117]]|
|2016-08-29 00:00:...|2180.379883|[[2180.379883]]|
|2016-08-26 00:00:...|2169.040039|[[2169.040039]]|
+--------------------+-----------+---------------+
only showing top 20 rows
*/
