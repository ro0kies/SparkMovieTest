package MySelfVersion

import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wys
  * @date 2020/5/23 - 10:37
  */
object MovieTest3 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)
    var master="local[2]"
    var name="movie analysice"
    if(args.length>1){
      master=args(0)
      name=args(1)
    }

    val conf=new SparkConf().setAppName(name).setMaster(master)
    val sc=new SparkContext(conf)

    val ratingsRdd=sc.textFile("F:\\IDEA\\SparkTest1\\MovieAnalyse\\src\\data\\ratings.dat")


    //todo 1.所有电影平均得分最高的前10部(分数：电影ID)
    val moiveInfo=ratingsRdd.map(_.split("::")).map(movie=>{
     // (movie(2).toInt,movie(1))
      (movie(1).toInt,(movie(2).toInt,1))   //ID,分数,1         最好表明类型，后续的函数总会把其中一个看做String
    })

    val top10Score=moiveInfo.reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))
      .map(t => (t._1,t._2._1/t._2._2.toDouble))
      .sortBy(p=>(p._2,-p._1),false)       //TODO 加上二次排序
      .take(10)
      .foreach(println)




    //2 todo 观看人数最多的前十部电影   (人数，电影id)
    val rdd=ratingsRdd.map(_.split("::"))
      .map(movie=>{
        //MovieID
        (movie(1),1)
      })

    val top10Count=rdd.reduceByKey(_+_)
      .sortBy(_._2,false)
      .take(10)
      .foreach(println)

  }
}
