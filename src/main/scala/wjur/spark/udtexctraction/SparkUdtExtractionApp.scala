package wjur.spark.udtexctraction

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Wojciech Jurczyk <wojtek.jurczyk@gmail.com>.
 */
object SparkUdtExtractionApp {
  def main(args: Array[String]) {
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("TestApp")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    val sqlContext: SQLContext = new SQLContext(sparkContext)

    val dataFrameWithUdt = createDataFrame(sqlContext)

    dataFrameWithUdt.show()
  }

  def createDataFrame(sqlContext: SQLContext) = {
    val data = Seq(
      TestUdtValue(1, 3.14),
      TestUdtValue(2, 0.0015),
      TestUdtValue(3, 0.0000926)
    )

    sqlContext.createDataFrame(data)
  }
}
