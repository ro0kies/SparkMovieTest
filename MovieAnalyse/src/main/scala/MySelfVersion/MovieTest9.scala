package MySelfVersion

import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wys
  * @date 2020/5/24 - 14:24
  */
object MovieTest9 {
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

    //分析不同职业对观看电影类型的影响
    //  (职业名，(电影类型，次数))
    // 用户与职位
    val userRdd=sc.textFile("F:\\IDEA\\SparkTest1\\MovieAnalyse\\src\\data\\users.dat")
    val occupationsRdd=sc.textFile("F:\\IDEA\\SparkTest1\\MovieAnalyse\\src\\data\\occupations.dat")
    val moviesRdd=sc.textFile("F:\\IDEA\\SparkTest1\\MovieAnalyse\\src\\data\\movies.dat")
    val ratingsRdd=sc.textFile("F:\\IDEA\\SparkTest1\\MovieAnalyse\\src\\data\\ratings.dat")

    val user=userRdd.map(_.split("::"))
      .map(x=>(x(3),x(0)))  //职业id，userid

    val rating=ratingsRdd.map(_.split("::"))
      .map(x=>(x(0),x(1)))   //userid,movieid

    val occ=occupationsRdd.map(_.split("::"))
      .map(x=>(x(0),x(1)))  //职业id,职业名


    //合并用户与职业
    val uoRdd=user.join(occ)  //   职业id，(userid,职业名)
      .map(x=>{(x._2._1,x._2._2)})     //userid,职业名

    //电影和电影类型
    val movieTypeRdd=moviesRdd.map(_.split("::"))
      .map(x=>(x(0),x(2)))    //编号，类型  (1, Adventure|Children's|Fantasy)
      .flatMapValues(types=>{
      types.split("\\|")  //（1，A）,(1,B)
    })


    val rdd=uoRdd.join(rating)    //userid,(movieid,occupationName)
        .map(item=>(item._2._2,item._2._1))   //movieid,occupationName
      .join(movieTypeRdd)     //movieid,(occupationName,type)
      .map(item=>(item._2._1,(item._1,item._2._2)))   //occupationName,(movieid,type)

    rdd.groupByKey()
      .flatMapValues(array=>{
        var a:Map[String,Int]=Map()
        array.foreach(item=>{
          if(a.contains(item._2)){
            var oldCount=a.getOrElse(item._2,0)+1
            a+=(item._2->oldCount)
          }else{
            a+=(item._2->1)
          }
        })
        a
      })
      .collect()
      .foreach(println)

  }
}
