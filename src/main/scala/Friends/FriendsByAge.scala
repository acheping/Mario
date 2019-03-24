package Friends

object FriendsByAge {
   def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rddFichier = sc.textFile("C:/.../fichierNbAmis.txt")

    val rddSansEntete = rddFichier.mapPartitionsWithIndex { (idx, iter) =>
      if (idx == 0) iter.drop(1)
      else iter
    }

    val rddSplit = rddSansEntete.map(line => (line.split(", ")(0), (line.split(", ")(1).toInt,1)))
    val totalByAge = rddSplit.reduceByKey((x,y)=>(x._1.toInt+y._1.toInt,x._2+y._2))
    val averageFriendsByAge = totalByAge.mapValues(x => x._1.toFloat / x._2)

    rddSplit.foreach(println)
    totalByAge.foreach(println)
    averageFriendsByAge.foreach(println)
  }
}
