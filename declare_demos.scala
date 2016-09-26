/*~/sparkapps/declare_demos.scala

This should show syntax on how to declare some types of objects.

Demo:
spark-shell -i declare_demos.scala
*/

val os: Iterable[String] = Iterable("a","b","c","d")
os

/* another:
ref:
http://spark.apache.org/docs/latest/ml-pipeline.html#example-estimator-transformer-and-param
*/
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row

// Prepare training data from a list of (label, features) tuples.
val training = spark.createDataFrame(Seq(
  (1.1, Vectors.dense(1.1, 0.1)),
  (0.2, Vectors.dense(1.0, -1.0)),
  (3.0, Vectors.dense(1.3, 1.0)),
  (1.0, Vectors.dense(1.2, -0.5))
)).toDF("label", "features")

training.show


