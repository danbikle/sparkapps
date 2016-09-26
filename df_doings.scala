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
