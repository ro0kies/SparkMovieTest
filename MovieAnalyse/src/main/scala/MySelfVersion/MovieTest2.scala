package MySelfVersion

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

//    val userBasic= userRdd.map(_.split("::")).map(user=>{
//      (user(0))
//    })
    val ratingBasic=ratingsRdd.map(_.split("::")).map(rating=>{
      (rating(0),1)
    })
    val movieBasic=moviesRdd.map(_.split("::")).map(movie=>{
      (movie(0),movie(1),movie(2))
    })
    //1.每个用户看的电影数量      TODO  没有去重，所以存在一个用户看一部电影多次
    val userSeeCount=ratingBasic.reduceByKey(_+_).sortBy(_._1).collect()
    userSeeCount.foreach(println)

    //2.电影信息
    println("MovieID     Title      Type")
    movieBasic.foreach(println)

  }
}
