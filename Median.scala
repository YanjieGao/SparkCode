import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext.rddToPairRDDFunctions
/**
 * Created by v-yanjga on 8/15/2014.
 */
object Median {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val n = 10000
    val data = spark.parallelize(1 to n, slices)
    val mappeddata =data.map(num => {
      (num/1000 , num)
    })
    val count = mappeddata.reduceByKey((a , b) => {
      a+b
    }).collect()
    val sum_count=count.map(data => {
      data._2
    }).sum
    var temp = 0
    var index = 0
    var mid = sum_count/2
    for( i <- 0 to 10) {
      temp=temp+count(i)
      if(temp >= mid) {
        index=i
        break
      }
    }
    val offset = temp - mid
    val result = mappeddata.filter(num => num._1 == index ).takeOrdered(offset)
    println("Median is " + result )
    spark.stop()
  }
}
