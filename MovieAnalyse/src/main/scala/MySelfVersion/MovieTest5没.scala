package MySelfVersion

import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wys
  * @date 2020/5/23 - 17:50
  */
object MovieTest5没 {
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

      //不同年龄段喜欢的电影的前10
    /** 年龄段
      * under 18: 1
      * 18 - 24: 18
      * 25 - 34: 25
      * 35 - 44: 35
      * 45 - 49: 45
      * 50 - 55: 50
      * 56 + 56
      */

    val userRdd=sc.textFile("F:\\IDEA\\SparkTest1\\MovieAnalyse\\src\\data\\users.dat")
    val ratingsRdd=sc.textFile("F:\\IDEA\\SparkTest1\\MovieAnalyse\\src\\data\\ratings.dat")

    var age="18"      //默认拿到的数据为String，如果不转为String的话，就要将数据转为Int
    println("年龄段为:"+age+"喜欢的前10")

    val userWithAge=userRdd.map(_.split("::"))
      .map(u=>(u(0),u(2)))      //userid,age段
      .filter(_._2.equals(age))   //过滤掉其他年龄段的

    userWithAge.cache()  //缓存
    //1.userwithAge.colle 然后broadcast      sc.broadcast(userWithAge.collect())
    //2. 做成2个rdd，然后join

    val ratings=ratingsRdd.map(_.split("::"))
      .map(  r=>(r(0),(r(1),r(2))) ) //(userid,(moiveid,score))


    val ratingWithAge=userWithAge.join(ratings) //(userid,(age),(moiveid,score))
      .map(i=>(i._2._2._1,(i._2._2._2.toDouble,1)))   //(moiveid,(score,1))
      .reduceByKey((x,y)=>{(x._1+y._1,x._2+y._2)})    //总分，个数
      .map(item=>{(item._2._1/item._2._2,(item._1,item._2._1,item._2._2))}) //(平均分,(movieid,总分，个数))
      .sortByKey(false)
      .take(10)
      .foreach(println)


    //利用广播变量来取代join的操作
    println("年龄在":+age+"的用户数据量"+userWithAge.count())

    val userArray=userWithAge.collect()   //userid,age段
    val broadcasetRef=sc.broadcast(userArray)
    val ageRef=sc.broadcast(age)

    //(userid,(moiveid,score))
    val ratingWithAgeRdd=ratings.filter(r=>{
      val userArray=broadcasetRef.value
      val age=ageRef.value
      userArray.contains(((r._1,age)))
    })


    println("用户的打分数据量有:"+ratingWithAgeRdd.count())




  }

}
