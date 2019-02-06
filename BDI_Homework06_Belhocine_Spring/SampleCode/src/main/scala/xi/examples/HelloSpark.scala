package xi.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object HelloSpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCount Reduce by Key")

    val sc = new SparkContext(conf)

    val tweetsPath = args(0)
    val outputDataset = args(1)

    val tweetsRaw: RDD[String] = sc.textFile(tweetsPath)

    val wordCounts = tweetsRaw.
      flatMap(line => line.split("\\s+")).
      groupBy(identity).
      map { case (word, words) => (word, words.size) }


    wordCounts.saveAsTextFile(outputDataset)


  } //main

}//HelloSpark


