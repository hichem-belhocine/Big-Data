ExtractTweetTextLocal:
	bin/spark-submit \
	--master local[*] \
	--class xi.examples.ExtractTweetText \
	~/SampleCode-assembly-1.0.jar \
	file:///home/alberto/mock_tweets \
	file:///home/alberto/mock_tweets_result.tsv


ExtractTweetText:
		bin/spark-submit \
		--master yarn \
		--deploy-mode cluster \
		--num-executors 50 \
		--driver-memory 1g \
		--executor-memory 1g \
		--class xi.examples.ExtractTweetText \
		~/SampleCode-assembly-1.0.jar \
		/user/ruslan/ruslan/all_tweets_no_gps \
		allTweetsText_15Feb_1.gz


WordCountLocal:
	bin/spark-submit \
	--master local[*] \
	--class xi.examples.HelloSpark \
	~/SampleCode-assembly-1.0.jar \
	file:///home/alberto/mock_tweets_result.tsv \
	file:///home/alberto/mock_tweets_result_words.tsv


WordCount:
	bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 5 \
    --driver-memory 1g \
    --executor-memory 1g \
    --class xi.examples.HelloSpark \
    ~/SampleCode-assembly-1.0.jar \
    file:///home/alberto/mock_tweets \
    file:///home/alberto/mock_tweets_words.tsv


Cache:
	bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 50 \
    --driver-memory 1g \
    --executor-memory 1g \
    --class xi.examples.Caching \
    ~/SampleCode-assembly-1.0.jar \
    /user/ruslan/ruslan/all_tweets_no_gps \
    tweetsICare/tweetText


Shell:
	bin/spark-shell \
	--master yarn \
	--deploy-mode client \
	--num-executors 3 \
	--driver-memory 1g \
	--executor-memory 1g

ToParquet:
	bin/spark-submit \
	--master yarn \
	--deploy-mode client \
	--num-executors 50 \
	--driver-memory 1g \
	--executor-memory 1g \
	--class xi.examples.ToParquet \
	~/SampleCode-assembly-1.0.jar \
	/user/ruslan/ruslan/all_tweets_no_gps \
	allTweetsText_15Feb_1.parquet