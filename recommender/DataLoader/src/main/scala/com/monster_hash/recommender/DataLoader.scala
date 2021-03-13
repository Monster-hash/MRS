package com.monster_hash.recommender

// 定义样例类
case class Movie(mid: Int,
                 name: String,
                 descri: String,
                 timelong: String,
                 issue: String,
                 shoot: String,
                 language: String,
                 genres: String,
                 actors: String,
                 directors: String)
case class Rating(uid: Int,
                  mid: Int,
                  score: Double,
                  timestamp: Int)
case class Tag(uid: Int,
               mid: Int,
               tag: String,
               timestamp: Int)
case class MongoConfig(uri:String,
                       db:String)
case class ESConfig(httpHosts:String,
                    transportHosts:String,
                    index:String,
                    clustername:String)

//把MongoDB和ES的配置封装成样例类
/**
 *
 * @param uri MongoDB连接
 * @param db MongoDB数据库
 */
case class MongoConfig(uri:String, db:String)

/**
 *
 * @param httpHosts             http主机列表，逗号分隔
 * @param transportHosts        transport主机列表，集群内部数据传输，逗号分隔
 * @param index                 需要操作的索引
 * @param clustername           集群名称，默认elasticsearch
 */
case class ESConfig(httpHosts:String, transportHosts:String, index:String, clustername:String)

object DataLoader {
  //定义常量
  val MOVIE_DATA_PATH = "D:\\MRS\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "D:\\MRS\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "D:\\MRS\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "localhost:9200",
      "es.transportHosts" -> "localhost:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "elasticsearch"
    )
    // 创建一个 SparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")

    //创建一个 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 在对 DataFrame 和 Dataset 进行操作许多操作都需要这个包进行支持,implicits._隐式转换
    import spark.implicits._
    //加载数据
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    //将 MovieRDD 装换为 DataFrame
    val movieDF = movieRDD.map(
      item =>{
      val attr = item.split("\\^")
      Movie(attr(0).toInt,attr(1).trim,attr(2).trim,attr(3).trim,attr(4).trim,
        attr(5).trim,attr(6).trim,attr(7).trim,attr(8).trim,attr(9).trim)
    }).toDF()
    val ratingRDD = null
    val tagRDD = null

    //数据预处理

    //将数据保存到MongoDB
    storeDataInMongoDB()

    //保存数据到ES
    storeDataInES()

    spark.stop()
  }
  def storeDataInMongoDB(): Unit ={

  }
  def storeDataInES(): Unit ={

  }

}
