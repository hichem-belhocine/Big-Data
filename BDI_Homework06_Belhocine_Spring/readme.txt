
TASK1:

bin/spark-submit --master local[*] --class xi.examples.WordCount  /home/bdi18_14/bundle/hadoop/SampleCode.jar /bdi_2018/bdi18_14/tweet_text.txt  /bdi_2018/bdi18_14/results/task1

TASK2:
bin/spark-submit --master local[*] --class xi.examples.HashTagCounter  /home/bdi18_14/bundle/hadoop/SampleCode.jar /bdi_2018/bdi18_14/tweet_sample_raw_data.txt   /bdi_2018/bdi18_14/results/task2

TASK3:
bin/spark-submit --master local[*] --class xi.examples.StopWordRemover  /home/bdi18_14/bundle/hadoop/SampleCode.jar /bdi_2018/bdi18_14/tweet_sample_raw_data.txt   /bdi_2018/bdi18_14/results/task3


