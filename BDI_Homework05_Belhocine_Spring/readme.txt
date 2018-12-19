Homework MapReduce Lab
Hichem Belhocine, Timo Spring
Group: bdi18_14
Output location: 	/bdi_2018/bdi18_14/output/ 
List output files: 	bin/hadoop fs -ls /bdi_2018/bdi18_14/output/

1) Run wordcount on NYTimes_articles: 
javac -classpath `bin/hadoop classpath` WordCount.java
jar cf wc.jar WordCount*class
bin/hadoop jar wc.jar WordCount /bdi_2018/data/NYTimes_articles /bdi_2018/bdi18_14/output/wordcount_1
bin/hadoop fs -cat /bdi_2018/bdi18_14/output/wordcount_1/*

2) & 3)
javac -classpath `bin/hadoop classpath` WordCount.java
jar cf wc.jar WordCount*class
bin/hadoop jar wc.jar WordCount /bdi_2018/data/NYTimes_articles /bdi_2018/bdi18_14/output/wordcount_2
bin/hadoop fs -cat /bdi_2018/bdi18_14/output/wordcount_2/*


4)
javac -classpath `bin/hadoop classpath` CountLiterals.java
jar cf cl.jar CountLiterals*class
bin/hadoop jar cl.jar CountLiterals /bdi_2018/data/btc09 /bdi_2018/bdi18_14/output/countliterals_4
bin/hadoop fs -cat /bdi_2018/bdi18_14/output/countliterals_4/*


5) 
javac -classpath `bin/hadoop classpath` CountDegrees.java
jar cf cd.jar CountDegrees*class
bin/hadoop jar cd.jar CountDegrees /bdi_2018/data/btc09 /bdi_2018/bdi18_14/output/countdegrees_5
bin/hadoop fs -cat /bdi_2018/bdi18_14/output/countdegrees_5/*
