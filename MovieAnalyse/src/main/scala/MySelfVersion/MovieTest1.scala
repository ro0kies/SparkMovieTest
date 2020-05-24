package MySelfVersion

import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wys
  * @date 2020/5/22 - 22:29
  */
object MovieTest1 {
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

    println("职业数:"+occupationsRdd.count())
    println("电影数:"+moviesRdd.count())
    println("用户数:"+userRdd.count())
    println("评分条数:"+ratingsRdd.count())

    //1. 每个职业下的用户信息 （职业编号，（人的编号，性别，年龄，邮编）,职业名）
    val userBasic=userRdd.map(_.split("::")).map(user=>{
      (user(3),(user(0),user(1),user(2),user(4)))
    })

    val occupations=occupationsRdd.map(_.split("::")).map(occupation=>{
      (occupation(0),occupation(1))
    })

    //合并
    val userInfo=userBasic.join(occupations)
    println("用户详情：(职业编号，(人的编号，性别，年龄，邮编),职业名)")

    userInfo.foreach(println)
    println("合并后一共有:"+userInfo.count()+"条记录")   //和用户数一致

    userRdd.unpersist()
    occupationsRdd.unpersist()
    moviesRdd.unpersist()
    ratingsRdd.unpersist()
    sc.stop()
  }
}
