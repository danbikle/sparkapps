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

