import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountLiterals {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//we need to put a delimiter for the string tokenizer, otherwise he separe all the words
			StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			while (itr.hasMoreTokens()) {
				String[] myWord = itr.nextToken().split("\t");
				//search if there is a literal
				if(myWord[2].matches("\".*\"")){
					word.set(myWord[0]);
					context.write(word, one);
				}			
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
			//veryfie if there is 5 or more literals
			if(sum >= 5){
				result.set(sum);
				context.write(key, result);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "countliterals");
		job.setJar("cl.jar");
		job.setJarByClass(CountLiterals.class);
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
