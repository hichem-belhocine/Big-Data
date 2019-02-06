package xi.examples


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}



object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCount")

    val sc = new SparkContext(conf)

    val tweetsPath = args(0)
    val outputDataset = args(1)

    val tweets: RDD[String] = sc.textFile(tweetsPath)

    val wordCounts = tweets.
      flatMap(line => line.split("\\s+").
        map(_.replaceAll("""[\p{Punct}]""", "")).//we remove all the punctuation from the strings
        map(text => text.toLowerCase)).//we put all characters in lowercase
      map(word => (word,1)).//We create the tuples
      reduceByKey(_ + _)//we compute the number of occurence


    wordCounts.saveAsTextFile(outputDataset)
  } //main

}
