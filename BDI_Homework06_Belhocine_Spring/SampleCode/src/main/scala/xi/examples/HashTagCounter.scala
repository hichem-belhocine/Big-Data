package xi.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object HashTagCounter {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("HashtagCounter")

    val sc = new SparkContext(conf)

    val tweetsPath = args(0)
    val outputDataset = args(1)

    val tweets: RDD[String] = sc.textFile(tweetsPath)

    val hashtagcount = tweets.
      flatMap(line => line.split("\\s+")).
      filter(string => string.matches("[#]\\S*")).
      map(tag => (tag,1)).
      reduceByKey(_ + _).
      sortBy(_._2,false)



    hashtagcount.saveAsTextFile(outputDataset)
  } //main


}
