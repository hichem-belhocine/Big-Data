import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;

public class CountDegrees {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Node>{
		private Text word = new Text();
		private Text object = new Text(); 
		public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
			//we need to put a delimiter for the string tokenizer, otherwise he separe all the words
			StringTokenizer itr = new StringTokenizer(values.toString(),"\n");
			while (itr.hasMoreTokens()) {
				String[] myWord = itr.nextToken().split("\t");
				Node node = new Node(); 
				if(myWord[2].matches("\".*\"")){
					node.setLiteral(1); 
					node.setOut(1); 
					word.set(myWord[0]);
					context.write(word, node);
				}else{
					node.setOut(1);  
					word.set(myWord[0]);
					context.write(word, node);

					object.set(myWord[2]); 
					context.write(object, new Node(0,0,1));
				}
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text,Node,Text,Node> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
			int litsum = 0;
			int outNb = 0;
			int inNb = 0;
			//compute the number of literals, in and out edges
			for (Node val : values) {
				litsum += val.getLiteral().get();
				outNb += val.getOut().get();
				inNb += val.getIn().get();
			}
			//veryfie if there is 10 or more literals
			if(litsum >= 10){
				context.write(key, new Node(inNb,outNb,litsum));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "count degrees");
		job.setJar("cd.jar");
		job.setJarByClass(CountDegrees.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Node.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	//A custom class that represente a node with incoming edges, out edges and a number of literals (use in reduce to select the node we want)
	public static class Node implements Writable{
		//must be IntWritable because because we need to override readFields and write and IntWirtable has already this function in the class
		private IntWritable in;
		private IntWritable out;
		private IntWritable literal;
		
		public Node(){
			in = new IntWritable(0);
			out = new IntWritable(0);
			literal = new IntWritable(0);
		}
		
		public Node(int i, int o, int lit){
			in = new IntWritable(i);
			out = new IntWritable(o);
			literal = new IntWritable(lit);
		}
		
		public void setIn(int i){
			in.set(i);
		}
		
		public void setOut(int o){
			out.set(o);
		}
		
		public void setLiteral(int l){
			literal.set(l);
		}
		
		public IntWritable getIn(){
			return in;
		}
		
		public IntWritable getOut(){
			return out;
		}
		
		public IntWritable getLiteral(){
			return literal;
		}
		
		@Override
		public void readFields(DataInput din) throws IOException {
			in.readFields(din);
			out.readFields(din);
			literal.readFields(din);
    	}
 
		@Override
		public void write(DataOutput dout) throws IOException {
			in.write(dout);
			out.write(dout);
			literal.write(dout);
		}
		
		@Override
    	public String toString() {
        	return "In:" + in + ",Out:" + out;
    	}
		
	}
}

