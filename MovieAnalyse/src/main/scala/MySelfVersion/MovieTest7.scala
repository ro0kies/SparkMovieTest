package MySelfVersion

import java.util.logging.{Level, Logger}
import java.util.regex.Pattern

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wys
  * @date 2020/5/24 - 10:50
  */
object MovieTest7 {
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

    //统计电影年份  输出格式 (年份，数量)
    val yearRdd=moviesRdd.map(_.split("::"))
      .map(x=>(x(1),1))     //电影名，1,
      .map(item=>{
      var moiveN=""
      var year=""
      val pattern=Pattern.compile("(.*)(\\(\\d{4}\\))") //Toy Story(1995)
      val matcher=pattern.matcher(item._1)
      if(matcher.find()){
        moiveN=matcher.group(1)
        year=matcher.group(2)
        year=year.substring(1,year.length-1)    //去掉两边的括号
      }
      if(year==""){
        (-1,1)
      }else{
        (year.toInt,1)    //返回的类型
      }

    })  //(year,(1,类型))


      .reduceByKey(_+_)
      .sortByKey()
      .collect()
      .foreach(println)
  }
}
