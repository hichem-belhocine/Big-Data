package hichem

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


class StopWordRemover {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("StopWordRemover")

    val sc = new SparkContext(conf)

    val tweetsPath = args(0)
    val outputDataset = args(1)

    val tweets: RDD[String] = sc.textFile(tweetsPath)

    val stopWorld = Array("a","able","about","across","after","all","almost","also","am","among","an","and","any","are","as","at","be","because","been","but","by","can","cannot","could","dear","did","do","does","either","else","ever","every","for","from","get","got","had","has","have","he","her","hers","him","his","how","however","i","if","in","into","is","it","its","just","least","let","like","likely","may","me","might","most","must","my","neither","no","nor","not","of","off","often","on","only","or","other","our","own","rather","said","say","says","she","should","since","so","some","than","that","the","their","them","then","there","these","they","this","tis","to","too","twas","us","wants","was","we","were","what","when","where","which","while","who","whom","why","will","with","would","yet","you","your","ain't","aren't","can't","could've","couldn't","didn't","doesn't","don't","hasn't","he'd","he'll","he's","how'd","how'll","how's","i'd","i'll","i'm","i've","isn't","it's","might've","mightn't","must've","mustn't","shan't","she'd","she'll","she's","should've","shouldn't","that'll","that's","there's","they'd","they'll","they're","they've","wasn't","we'd","we'll","we're","weren't","what'd","what's","when'd","when'll","when's","where'd","where'll","where's","who'd","who'll","who's","why'd","why'll","why's","won't","would've","wouldn't","you'd","you'll","you're","you've")

    val filter1 = tweets.
      map(line => line.split("\t")).
      filter(split => split.length == 4).
      map { split =>
        val Array(location, tweetId, text, date) = split
        (location, text)
      }.
      filter{ case (location,_) => location == "San Francisco, CA"}
    
    val filter2 = tweets.
      map(line => line.split("\t")).
      filter(split => split.length == 4).
      map { split =>
        val Array(location, tweetId, text, date) = split
        (location, text)
      }.
      filter{ case (location,_) => location == "Chicago, IL"}
      
    val alltext = filter1 ++ filter2
    
    
    alltext.
    map{ text =>
      var temptext:String = text._2.toLowerCase
      for(reg <- stopWorld){
        val pattern = "\\b"+reg+"\\b\\s?"
        temptext = pattern.r.replaceAllIn(temptext,"")
      }
      temptext
    }.
    saveAsTextFile(outputDataset)

  } //main

}
