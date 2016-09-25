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
