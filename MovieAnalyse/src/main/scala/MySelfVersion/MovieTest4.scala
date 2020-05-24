package MySelfVersion

import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wys
  * @date 2020/5/23 - 12:57
  */
object MovieTest4 {
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
    val userRdd=sc.textFile("F:\\IDEA\\SparkTest1\\MovieAnalyse\\src\\data\\users.dat")

    //男性看的最多的10部
    val man=userRdd.map(_.split("::")).map(m=>{
      (m(0),m(1))
    }).filter(_._2=="M")
      .map(a=>{
        (a._1,1)    //useid
      })

    val rating=ratingsRdd.map(_.split("::")).map(r=>{
      (r(0),(r(1).toInt,1))   //userid,movieid,1
    })

    val rdd=man.join(rating)     // (userid,(1),(movieid,1))
                  // ( moiveid ,    1)
      .map(item=>{(item._2._2._1,item._2._2._2)})

    println("男性看的最多10部")
    var result=rdd.reduceByKey(_+_)
      .sortBy(_._2,false)
      .take(10)
      .foreach(println)

    //女性看的最多10部电影
    val women=userRdd.map(_.split("::")).map(m=>{
      (m(0),m(1))
    }).filter(_._2=="F")
      .map(a=>{
        (a._1,1)    //useid
      })

    val rating2=ratingsRdd.map(_.split("::")).map(r=>{
      (r(0),(r(1).toInt,1))   //userid,movieid,1
    })

    val rdd2=man.join(rating2)       // (userid,(1),(movieid,1))
      // (           moiveid ,    1)
      .map(item=>{(item._2._2._1,item._2._2._2)})

    println("女性看的最多10部电影")
    var result2=rdd2.reduceByKey(_+_)
      .sortBy(_._2,false)
      .take(10)
      .foreach(println)





  }
}
