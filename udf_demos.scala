/* ~/sparkapps/udf_demos.scala
This script should demoonstrate some calls to udf()

Demo:
spark-shell -i udf_demos.scala
*/
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row

// Ref:
// http://spark.apache.org/docs/latest/ml-pipeline.html
// Prepare training data from a list of (label, features) tuples.

val x1df=spark.createDataFrame(Seq((1.0,Vectors.dense(0.0,1.1,0.1)),(0.0,Vectors.dense(2.0,1.0,-1.0)),(0.0,Vectors.dense(2.0,1.3,1.0)),(1.0,Vectors.dense(0.0,1.2,-0.5)))).toDF("label","features")

val change_label = udf((label:Float)=> {if (label==1.0) 0.0 else 1.0})

val x2df = x1df.withColumn("new_label",change_label(col("label")))

x2df.show
/*
+-----+--------------+---------+
|label|      features|new_label|
+-----+--------------+---------+
|  1.0| [0.0,1.1,0.1]|      0.0|
|  0.0|[2.0,1.0,-1.0]|      1.0|
|  0.0| [2.0,1.3,1.0]|      1.0|
|  1.0|[0.0,1.2,-0.5]|      0.0|
+-----+--------------+---------+
*/

val coder2: (Float  => String) = (arg: Float)  => {if (arg == 1.0) "positive" else "negative"}

val posneg = udf((arg:Float) => {coder2(arg)})

val x2df = x1df.withColumn("string_label",posneg(col("label")))

x2df.show
/*
+-----+--------------+------------+
|label|      features|string_label|
+-----+--------------+------------+
|  1.0| [0.0,1.1,0.1]|    positive|
|  0.0|[2.0,1.0,-1.0]|    negative|
|  0.0| [2.0,1.3,1.0]|    negative|
|  1.0|[0.0,1.2,-0.5]|    positive|
+-----+--------------+------------+
*/
