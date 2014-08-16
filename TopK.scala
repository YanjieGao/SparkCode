/**
 * Created by Administrator on 2014/8/16.
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object TopK {
  def main(args:Array[String]) {
    //进行WordCount统计出最高频的查询
    val spark = new SparkContext("local", "TopK",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val count = spark.parallelize(1 to n, slices).flatMap(line =>
      line.split(" ")).map(word =>
      (word, 1)).reduceByKey(_ + _)
    //统计RDD每个分区内的Topk查询
    val topk = count.mapPartitions(iter => {
      while(iter.hasNext) {
        putToHeap(iter.next())
      }
      getHeap().iterator
    }
    ).collect()
    //将每个分区内统计出的Topk查询合并后的集合，统计出TopK查询
    val iter = topk.iterator
    while(iter.hasNext) {
      putToHeap(iter.next())
    }
    val outiter=getHeap().iterator
    //输出TopK的查询值
    println("Topk 值 ：")
    while(outiter.hasNext) {
     println("\n查询 ："+outiter.next()._1+" 查询数 ："+outiter.next()._2)
    }
    spark.stop()
  }
}
def putToHeap(iter : (String, Int)) {
  //新数据加入含k个元素的堆中
}
def getHeap(): Array[(String, Int)] = {
  //获取含k个元素的堆中的元素
  val a=new Array[(String, Int)]()
}
