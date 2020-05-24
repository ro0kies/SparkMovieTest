package MySelfVersion

import java.util.logging.{Level, Logger}
import java.util.regex.Pattern

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wys
  * @date 2020/5/24 - 10:50
  */
object MovieTest8{
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

    //统计电影年份  输出格式 (年份，数量)  ->分析每年度不同类型的电影生产总数
    val yearRdd=moviesRdd.map(_.split("::"))
      .map(x=>(x(1),(1,x(2))))     //电影名，(1,类型)
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
        (-1,item._2)
      }else{
        (year.toInt,item._2)    //返回的类型
      }

    })  //(year,(1,类型))
      .groupByKey() //(年份,[(1,type),(1,type)])
      .flatMapValues(arr=>{     //(年份,[(1,type)),(年份,(1,type)])
      var a:Map[String,Int]=Map()//类型：次数
      arr.foreach(item=>{
        var count=item._1
        var types=item._2.split("\\|")
        for(t<-types){
          if(a.contains(t)){
            var oldCount=a.getOrElse(t,0)+1
            a+=(t->oldCount)
          }else{
            a+=(t->1)
          }
        }
      })
      a
    })
      //(year,(类型，次数))
      .sortByKey()
      .collect()
      .foreach(println)

  }
}
