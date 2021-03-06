/* ~/sparkapps/linr10/linr11.scala
This script should download prices and predict daily direction of GSPC.
It should generate a label which I assume to be dependent on price calculations.
A label should classify an observation as down or up. Down is 0.0, up is 1.0.
It should generate independent features from slopes of moving averages of prices.
It should create a Linear Regression model from many years of features.
Demo:
spark-shell -i linr11.scala
*/

import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

// I should declare training_period, test_period

val training_period = " WHERE Date BETWEEN'1986-01-01'AND'2016-01-01' "
val test_period = " WHERE Date BETWEEN'2016-01-01'AND'2017-01-01' "

// I should get prices:

import sys.process._

"/usr/bin/curl -o /tmp/gspc.csv http://ichart.finance.yahoo.com/table.csv?s=%5EGSPC"!

val sqlContext = new SQLContext(sc)

val dp10df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("/tmp/gspc.csv")

dp10df.createOrReplaceTempView("tab")

spark.sql("SELECT COUNT(Date),MIN(Date),MAX(Date),MIN(Close),MAX(Close)FROM tab").show

// I should compute a label I can use to classify observations.

var sqls="SELECT Date,Close,LEAD(Close,1)OVER(ORDER BY Date) leadp FROM tab ORDER BY Date"

val dp11df=spark.sql(sqls);dp11df.createOrReplaceTempView("tab")

var sqls="SELECT Date,Close,100*(leadp-Close)/Close pctlead FROM tab ORDER BY Date"

val dp12df=spark.sql(sqls);dp12df.createOrReplaceTempView("tab")

var sqls = "SELECT Date, Close, pctlead"
sqls=sqls++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mavg2"
sqls=sqls++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS mavg3"
sqls=sqls++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS mavg4"
sqls=sqls++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS mavg5"
sqls=sqls++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS mavg6"
sqls=sqls++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS mavg7"
sqls=sqls++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) AS mavg8"
sqls=sqls++",AVG(Close)OVER(ORDER BY Date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS mavg9"
sqls=sqls++" FROM tab ORDER BY Date"

val dp13df=spark.sql(sqls);dp13df.createOrReplaceTempView("tab")

// In Linear Regression, I should use pctlead as the label:

var sqls = "SELECT Date, Close, pctlead, pctlead label             "
sqls=sqls++",(mavg2-LAG(mavg2,1)OVER(ORDER BY Date))/mavg2 AS slp2 "
sqls=sqls++",(mavg3-LAG(mavg3,1)OVER(ORDER BY Date))/mavg3 AS slp3 "
sqls=sqls++",(mavg3-LAG(mavg4,1)OVER(ORDER BY Date))/mavg3 AS slp4 "
sqls=sqls++",(mavg3-LAG(mavg5,1)OVER(ORDER BY Date))/mavg3 AS slp5 "
sqls=sqls++",(mavg3-LAG(mavg6,1)OVER(ORDER BY Date))/mavg3 AS slp6 "
sqls=sqls++",(mavg3-LAG(mavg7,1)OVER(ORDER BY Date))/mavg3 AS slp7 "
sqls=sqls++",(mavg3-LAG(mavg8,1)OVER(ORDER BY Date))/mavg3 AS slp8 "
sqls=sqls++",(mavg3-LAG(mavg9,1)OVER(ORDER BY Date))/mavg3 AS slp9 "
sqls=sqls++" FROM tab ORDER BY Date"

val dp15df=spark.sql(sqls)

val fill_vec = udf((slp2:Float,slp3:Float,slp4:Float,slp5:Float,slp6:Float,slp7:Float,slp8:Float,slp9:Float)=> {Vectors.dense(slp2,slp3,slp4,slp5,slp6,slp7,slp8,slp9)})

val dp16df = dp15df.withColumn("features",fill_vec(col("slp2"),col("slp3"),col("slp4"),col("slp5"),col("slp6"),col("slp7"),col("slp8"),col("slp9")))

dp16df.createOrReplaceTempView("tab")

// I should create a LinearRegression instance. This instance is an 'Estimator'.

val lr = new LinearRegression()

lr.setMaxIter(1234).setRegParam(0.01)

// I should gather observations to learn from:
dp16df.createOrReplaceTempView("tab")

val train_df = spark.sql("SELECT * FROM tab"++training_period)

/*I should fit a LinearRegression model to observations.
This uses the parameters stored in lr.*/

val model1 = lr.fit(train_df)

val test_df = spark.sql("SELECT * FROM tab"++test_period)

/* I should predict. It is convenient that predictions_df will contain a copy of test_df.*/

val predictions_df = model1.transform(test_df)
predictions_df.createOrReplaceTempView("tab")

// Long-only effectiveness:
spark.sql("SELECT SUM(pctlead) eff_lo FROM tab").show

// Effectiveness of negative predictions:
spark.sql("SELECT -SUM(pctlead) eff_np FROM tab WHERE prediction<0").show

// Effectiveness of positive predictions:
spark.sql("SELECT SUM(pctlead) eff_pp FROM tab WHERE prediction>0").show

// True Positive Count:
spark.sql("SELECT COUNT(Date) tpc FROM tab WHERE prediction>0 AND pctlead>0").show

// True Negative Count:
spark.sql("SELECT COUNT(Date) tnc FROM tab WHERE prediction<0 AND pctlead<0").show

// False Positive Count:
spark.sql("SELECT COUNT(Date) fpc FROM tab WHERE prediction>0 AND pctlead<0").show

// False Negative Count:
spark.sql("SELECT COUNT(Date) fnc FROM tab WHERE prediction<0 AND pctlead>0").show

// prediction report:
// predictions_df.select("Date","Close","pctlead","label","prediction").show(255)

// eff logic
val eff = udf((pctlead:Float,prediction:Double)=> {if (prediction>0) pctlead else -pctlead})

val p2df = predictions_df.withColumn("effectiveness",eff(col("pctlead"),col("prediction")))
p2df.select("Date","Close","pctlead","effectiveness","label","prediction").show(255)
