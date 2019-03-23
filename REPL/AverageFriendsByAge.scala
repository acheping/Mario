val amis = sc.textFile ("hdfs://quickstart.cloudera:8020/user/arthur/testAmis.txt")

val amisNoHead = amis.mapPartitionsWithIndex{ (idx, iter) => if (idx == 0) iter.drop(1) else iter }

val amisPair = amisNoHead.map(_.split(";")).map(p => (p(0),(p(1).toInt,1)))

val amisPair2 = amisPair.reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))

val amisMoyenneAge = amisPair2.map(a=>(a._1, a._2._1/a._2._2))
