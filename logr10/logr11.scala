/* ~/sparkapps/logr10/logr11.scala
This script should download some data.
It should generate a label which I assume to be dependent on price calculations.
A label should classify an observation as down or up. Down is 0.0, up is 1.0.
It should generate independent features from slopes of moving averages of prices.
It should create a Logistic Regression model from many years of features.
This script is a less-verbose enhancement of logr10.scala.
Demo:
spark-shell -i logr11.scala
*/

import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._


// I should get prices:
import sys.process._
"/usr/bin/curl -o /tmp/gspc.csv http://ichart.finance.yahoo.com/table.csv?s=%5EGSPC"!

val sqlContext = new SQLContext(sc)

val gspc10_df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("/tmp/gspc.csv")

// I should Register the DataFrame as a SQL temporary view
gspc10_df.createOrReplaceTempView("gspc10_table")

var sql_s="SELECT Date,Close,LEAD(Close,1)OVER(ORDER BY Date) leadp FROM gspc10_table ORDER BY Date"

val gspc11_df = spark.sql(sql_s)

gspc11_df.createOrReplaceTempView("gspc11_table")

var sql_str="SELECT Date,Close,100*(leadp-Close)/Close pctlead FROM gspc11_table ORDER BY Date"

val gspc12_df = spark.sql(sql_str)

gspc12_df.createOrReplaceTempView("gspc12_table")

// I should get moving avg of pctlead looking back 25 years.
gspc12_df.createOrReplaceTempView("gspc12a_table")

var sqls="SELECT Date,Close,pctlead,AVG(pctlead)OVER(ORDER BY Date ROWS BETWEEN 25*252 PRECEDING AND CURRENT ROW)avgpctlead FROM gspc12a_table ORDER BY Date"

val gspc12a_df = spark.sql(sqls)
gspc12a_df.createOrReplaceTempView("gspc12b_table")
// I should have avgpctlead now(inside gspc12b_table). I should use it later as a class boundry.

var sql_str = "SELECT Date, Close, pctlead"
sql_str=sql_str++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mavg2"
sql_str=sql_str++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS mavg3"
sql_str=sql_str++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS mavg4"
sql_str=sql_str++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS mavg5"
sql_str=sql_str++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS mavg6"
sql_str=sql_str++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS mavg7"
sql_str=sql_str++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) AS mavg8"
sql_str=sql_str++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS mavg9"
sql_str=sql_str++" FROM gspc12_table ORDER BY Date"
val gspc13_df = spark.sql(sql_str)

gspc13_df.createOrReplaceTempView("gspc13_table")
var sql_str = "SELECT Date, Close, pctlead"
sql_str=sql_str++",(mavg2-LAG(mavg2,1)OVER(ORDER BY Date))/mavg2 AS slp2 "
sql_str=sql_str++",(mavg3-LAG(mavg3,1)OVER(ORDER BY Date))/mavg3 AS slp3 "
sql_str=sql_str++",(mavg3-LAG(mavg4,1)OVER(ORDER BY Date))/mavg3 AS slp4 "
sql_str=sql_str++",(mavg3-LAG(mavg5,1)OVER(ORDER BY Date))/mavg3 AS slp5 "
sql_str=sql_str++",(mavg3-LAG(mavg6,1)OVER(ORDER BY Date))/mavg3 AS slp6 "
sql_str=sql_str++",(mavg3-LAG(mavg7,1)OVER(ORDER BY Date))/mavg3 AS slp7 "
sql_str=sql_str++",(mavg3-LAG(mavg8,1)OVER(ORDER BY Date))/mavg3 AS slp8 "
sql_str=sql_str++",(mavg3-LAG(mavg9,1)OVER(ORDER BY Date))/mavg3 AS slp9 "
sql_str=sql_str++" FROM gspc13_table ORDER BY Date"
val gspc14_df = spark.sql(sql_str)

// I should get the value of avgpctlead for last day of 2015

var sqls="SELECT avgpctlead FROM gspc12b_table WHERE Date=(SELECT MAX(Date)FROM gspc12b_table WHERE Date<'2016-01-01')"

val gspc12b_df    = spark.sql(sqls)
val class_boundry = gspc12b_df.first()(0).asInstanceOf[Double] // Should be near 0.035

// I should compute label from pctlead:
val pctlead2label = udf((pctlead:Float)=> {if (pctlead>class_boundry) 1.0 else 0.0}) 

// I should add the label to my DF of observations:
val gspc17_df = gspc14_df.withColumn("label",pctlead2label(col("pctlead")))

// NOT NEEDED ? gspc17_df.createOrReplaceTempView("gspc17_table")

// I should copy slp-values into Vectors.dense():

val fill_vec = udf((slp2:Float,slp3:Float,slp4:Float,slp5:Float,slp6:Float,slp7:Float,slp8:Float,slp9:Float)=> {Vectors.dense(slp2,slp3,slp4,slp5,slp6,slp7,slp8,slp9)})

// I should see if I can replace Doubles with Floats

val gspc19_df = gspc17_df.withColumn("features",fill_vec(col("slp2"),col("slp3"),col("slp4"),col("slp5"),col("slp6"),col("slp7"),col("slp8"),col("slp9")))

// I should create a LogisticRegression instance. This instance is an 'Estimator'.
val lr = new LogisticRegression()

lr.setMaxIter(1234).setRegParam(0.01)

// I should gather observations to learn from:

val train_df = gspc19_df.filter($"Date" > "1986-01-01").filter($"Date" < "2016-01-01").select("label","features")

/*I should fit a LogisticRegression model to observations.
This uses the parameters stored in lr.*/
val model1 = lr.fit(train_df)
// Above line will fail with ugly error if train_df has any nulls.

val test_df = gspc19_df.filter($"Date" > "2016-01-01").filter($"Date" < "2017-01-01")

/* I should predict. It is convenient that predictions_df will contain a copy of test_df.*/
val predictions_df = model1.transform(test_df)

// I should report.
predictions_df.createOrReplaceTempView("predictions_table")

// Long-only effectiveness:
spark.sql("SELECT SUM(pctlead) eff_lo FROM predictions_table").show

// Effectiveness of negative predictions:
spark.sql("SELECT -SUM(pctlead) eff_np FROM predictions_table WHERE prediction = 0.0").show

// Effectiveness of positive predictions:
spark.sql("SELECT SUM(pctlead) eff_pp FROM predictions_table WHERE prediction = 1.0").show

// True Positive Count:
spark.sql("SELECT COUNT(Date) tpc FROM predictions_table WHERE prediction=1.0 AND pctlead>0").show

// True Negative Count:
spark.sql("SELECT COUNT(Date) tnc FROM predictions_table WHERE prediction=0.0 AND pctlead<0").show

// False Positive Count:
spark.sql("SELECT COUNT(Date) fpc FROM predictions_table WHERE prediction=1.0 AND pctlead<0").show

// False Negative Count:
spark.sql("SELECT COUNT(Date) fnc FROM predictions_table WHERE prediction=0.0 AND pctlead>0").show
