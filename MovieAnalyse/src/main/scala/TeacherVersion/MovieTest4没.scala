package TeacherVersion

import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wys
  * @date 2020/5/23 - 19:47
  */
object MovieTest4没 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)
    var master = "local[2]"
    var name = "movie analysice"
    if (args.length > 1) {
      master = args(0)
      name = args(1)
    }

    val conf = new SparkConf().setAppName(name).setMaster(master)
    val sc = new SparkContext(conf)

    val ratingsRdd = sc.textFile("F:\\IDEA\\SparkTest1\\MovieAnalyse\\src\\data\\ratings.dat")
    val userRdd = sc.textFile("F:\\IDEA\\SparkTest1\\MovieAnalyse\\src\\data\\users.dat")

    //男性评分最高的10部电影

    val man=userRdd.map(_.split("::"))
      .map(m=>{(m(0),m(1))})    //userid,性别

    val ratings=ratingsRdd.map(_.split("::"))
      .map(r=>{(r(0),(r(1),r(2)))}) //(userid,(moiveid,score))

    val ratingWithMan=man.join(ratings) //(userid,(性别),(moiveid,score))
      .map(item=>{(item._2._2._1,(item._1,item._2._1,item._2._2._2.toInt,1))})  //(moiveid,(userid,性别,score,1))
      .reduceByKey((x,y)=>(x._1,x._2,x._3+y._3,y._4+y._4)) //movieid,
     // .map(i=>{(i.)})












  }

}
