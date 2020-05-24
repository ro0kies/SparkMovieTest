package TeacherVersion

import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wys
  * @date 2020/5/23 - 10:01
  */
object MovieTest2 {
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

    val userRdd=sc.textFile("F:\\IDEA\\SparkTest1\\MovieAnalyse\\src\\data\\users.dat")
    val occupationsRdd=sc.textFile("F:\\IDEA\\SparkTest1\\MovieAnalyse\\src\\data\\occupations.dat")
    val moviesRdd=sc.textFile("F:\\IDEA\\SparkTest1\\MovieAnalyse\\src\\data\\movies.dat")
    val ratingsRdd=sc.textFile("F:\\IDEA\\SparkTest1\\MovieAnalyse\\src\\data\\ratings.dat")

    userRdd.cache()
    occupationsRdd.cache()
    moviesRdd.cache()
    ratingsRdd.cache()
    val userid=18
    //1.某个用户看的电影数量
    val ratingBasic=ratingsRdd.map(_.split("::"))
      .map(r=>{
      (r(1),r(0).toInt) //movieId,userId
    }).filter(_._2==userid)

     println(userid+"观看的电影数:"+ratingBasic.count())
    println(userid+"看过的电影详情:")
    val movieBasic=moviesRdd.map(_.split("::"))
      .map(movie=>{
      (movie(0),(movie(1),movie(2)))    //movieID,(title,type)
    })

    val res=ratingBasic.join(movieBasic)  //    (moiveid,(userid),(title,type))           展平
      .map(item=>{(item._1, item._2._1,item._2._2._1,item._2._2._2)})
      .foreach(println)


  }
}
