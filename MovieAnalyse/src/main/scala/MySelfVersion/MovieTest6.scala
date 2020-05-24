package MySelfVersion

import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @author wys
  * @date 2020/5/23 - 15:10
  */
object MovieTest6 {
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

    val moviesRdd=sc.textFile("F:\\IDEA\\SparkTest1\\MovieAnalyse\\src\\data\\movies.dat")

    //不同类型的电影总数
    val movieType=moviesRdd
      .map(_.split("::"))
      .map(m=>{(m(0),m(2))})    //1,a|b =>(1,a) （1，b）//movieid,type
//      .map(_._2.split("\\|"))
//      .map(x=>(x,1))
//      .flatMapValues()
      .flatMapValues(types=>{types.split("\\|")})   //(1,a) （1，b）
      .map(x=>(x._2,1))    //(a,1) （b，1）
      .reduceByKey(_+_)
      .foreach(println)






  }
}
