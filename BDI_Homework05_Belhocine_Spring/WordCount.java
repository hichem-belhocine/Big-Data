import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.lang.StringEscapeUtils;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				//a temporary string to work with for the replacement
				String myWord = word.toString();
				//put in lowercase
				myWord = myWord.toLowerCase();
				//change html entities to utf8 characters
				myWord = StringEscapeUtils.unescapeHtml(myWord);
				// clean the word from special characters, punctuations, digits etc. 
				String cleanedWord = myWord.replaceAll(
					"([^a-zA-Z0-9_\\-\\&\\.\\'\\’\\@])" +  // remove all non-word characters except for &, dots, appostrophes and hyphens
					"|(\\d(?!\\w*\\-))" + // remove all digits that are not part of words, e.g. 30th-anniversary is left untouched. 
					"|(\\'|\\’)s?(?!t)" + // remove all appostrophes and abbreviations such as 's but leave 't untouched so e.g. Tony's becomes Tony but won't remains won't
					"|(^(\\-|\\&|\\.|\\_)+)" + // remove all special characters that are not dots, hyphens, & underscore that are not used in words
					"|((((\\-|\\&|\\_)+)|((?<=\\w{2})\\.+))$)|(?<=[^a-zA-Z])&(?=[a-zA-Z])" + 
					"|((?<=[a-zA-Z])&(?![a-zA-Z]))|((?<![a-zA-Z])&(?![a-zA-Z]))|" + // remove all & signs except if they are used in words e.g. m&m is left untouched
					"((?<=\\d)\\.(?=\\d))" + // removing all dots used in numbers e.g. 1.23
                    "|((?<=\\w{2})\\.\\W*$)", ""); // removing all dots at the end of the string without touching abbreviations
				if(cleanedWord.length() > 1){ // exclude single characters since they don't build words 
					word.set(cleanedWord);
					context.write(word, one);
				}; 
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJar("wc.jar");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
