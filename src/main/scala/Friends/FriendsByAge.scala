package Friends

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FriendsByAge {
   def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Friends By Age").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val friends = sc.textFile("D:\\Homeware\\DEV\\Scala-Training\\src\\resources\\testAmis.txt")

    val header = friends.first
    val friendsWithoutHead = friends.filter(_ != header)

    val friendsSplited: RDD[(String, (Int, Int))] = friendsWithoutHead.map(_.split(";")).map(line => (line(0), (line(1).toInt, 1)))
    val totalByAge: RDD[(String, (Int, Int))] = friendsSplited.reduceByKey((x,y)=>(x._1 + y._1, x._2 + y._2))
    val averageFriendsByAge: RDD[(String, Float)] = totalByAge.mapValues(x => x._1.toFloat / x._2)

    friendsSplited.foreach(println)
    totalByAge.foreach(println)
    averageFriendsByAge.foreach(println)
  }
}
