package csd1045.atd1;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.FileSystem;

public class StopWords extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(StopWords.class);
	private static final String OUTPUT_PATH = "intermediate_output";
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new StopWords(), args);
		System.exit(res);
	}

  	public int run(String[] args) throws Exception {
  		Configuration conf = getConf();
  		FileSystem fs = FileSystem.get(conf);
  		Job job1 = Job.getInstance(conf, "wordcount");
  		job1.setJarByClass(this.getClass());
  		
  		job1.setMapperClass(MapWordCount.class);
  		job1.setReducerClass(ReduceWordCount.class);
  		
  		job1.setOutputKeyClass(Text.class);
  		job1.setOutputValueClass(IntWritable.class);

  		job1.setInputFormatClass(TextInputFormat.class);
  		job1.setOutputFormatClass(TextOutputFormat.class);  		
  		TextInputFormat.addInputPath(job1, new Path(args[0]));
  		TextOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));
  		
  		job1.waitForCompletion(true);
  		
  		
  		
  		Job job2 = Job.getInstance(conf, "somethingelse");
  		job2.setJarByClass(this.getClass());
  		
  		job2.setMapperClass(MapStopWords.class);
  		job2.setReducerClass(ReduceStopWords.class);
  		
  		job2.setOutputKeyClass(IntWritable.class);
  		job2.setOutputValueClass(Text.class);

  		job2.setInputFormatClass(TextInputFormat.class);
  		job2.setOutputFormatClass(TextOutputFormat.class); 
  		
  		TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
  		TextOutputFormat.setOutputPath(job2, new Path(args[1]));
  	  
  		return job2.waitForCompletion(true) ? 0 : 1;
  		
  		//return job1.waitForCompletion(true) ? 0 : 1;
  		
  		/*Job job = Job.getInstance(getConf(), "stopwords");
  		job.setJarByClass(this.getClass());
  		FileInputFormat.addInputPath(job, new Path(args[0]));
  		FileOutputFormat.setOutputPath(job, new Path(args[1]));
  		job.setMapperClass(Map.class);
  		job.setCombinerClass(Reduce.class);
  		job.setReducerClass(Reduce.class);
  		job.setOutputKeyClass(Text.class);
  		job.setOutputValueClass(IntWritable.class);
  		return job.waitForCompletion(true) ? 0 : 1;*/
  	}

  	public static class MapWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
	  
	  	private final static IntWritable one = new IntWritable(1);
	  	//private boolean caseSensitive = false;
	  	private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
	  	private static final Pattern ALPHA_NUMERIC = Pattern.compile("[^ a-zA-Z0-9]");
	  	/*protected void setup(Context context)throws IOException, InterruptedException {
	  		Configuration config = context.getConfiguration();
	  		this.caseSensitive = config.getBoolean("stopwords.case.sensitive", false);
	  	}*/
	  	public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
	  		String line = value.toString().toLowerCase();
	  		String words[] = WORD_BOUNDARY.split(line);
	  		for(String word : words){
	  			if(!ALPHA_NUMERIC.matcher(word).find() && !word.isEmpty())
	  				context.write(new Text(word), new IntWritable(1));
	  		}
	  	}
  	}

  	public static class MapStopWords extends Mapper<LongWritable, Text, IntWritable, Text> {
  	  
	  	public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
	  		String line = value.toString();
	  		String words[] = line.split("\\s+");
	  		context.write(new IntWritable(Integer.parseInt(words[1])), new Text(words[0]));
	  	}
  	}
  	
  	public static class ReduceWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
	  	@Override
	  	public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
	  		int sum = 0;
	  		for (IntWritable count : counts) {
	  			sum += count.get();
	  		}
	  		if(sum>4000)
	  			context.write(word, new IntWritable(sum));
	  	}
  	}
  	
  	public static class ReduceStopWords extends Reducer<IntWritable, Text, Text, IntWritable> {
	  	public void reduce(IntWritable counts, Text word, Context context) throws IOException, InterruptedException {
	  		context.write(word, counts);
	  	}
  	}
  	
  	/*public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	  	@Override
	  	public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
	  		int sum = 0;
	  		for (IntWritable count : counts) {
	  			sum += count.get();
	  		}
	  		context.write(word, new IntWritable(sum));
	  	}
  	}*/
}
