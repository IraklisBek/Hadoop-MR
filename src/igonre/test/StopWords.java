package igonre.test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;


public class StopWords extends Configured implements Tool {



	public static class IntComparator extends WritableComparator {

		public IntComparator() {
			super(IntWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2,
				int s2, int l2) {
			Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
			Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();
			return v1.compareTo(v2) * (-1);
		}
	}

	private static final Logger LOG = Logger.getLogger(StopWords.class);
	private static final String OUTPUT_PATH = "intermediate_output";
	

	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new StopWords(), args);
		System.exit(res);
	}

  	public int run(String[] args) throws Exception {
  		Configuration conf = getConf();
  		//conf.setInt("mapred.tasktracker.map.tasks.maximum", 6); 
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

  		

  		Job job2 = Job.getInstance(conf, "stop_words");
  		job2.setJarByClass(this.getClass());

  		job2.setMapperClass(MapStopWords.class);
  		job2.setReducerClass(ReduceTopKStopWords.class);
  		job2.setNumReduceTasks(1);//1 reducer because we are facing a sorting problem

  		job2.setOutputKeyClass(IntWritable.class);
  		job2.setOutputValueClass(Text.class);
  		
  		job2.setInputFormatClass(KeyValueTextInputFormat.class);
  		job2.setOutputFormatClass(TextOutputFormat.class); 
  		
  		
  		job2.setSortComparatorClass(IntComparator.class);
  		
  		TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
  		TextOutputFormat.setOutputPath(job2, new Path(args[1]));

  		boolean finished = job2.waitForCompletion(true);
  		job2.waitForCompletion(true);
  		try {
  			FileUtils.forceDelete(new File("/home/cloudera/workspace/ATD1/"+OUTPUT_PATH));
    	}catch(Exception e){
    		e.printStackTrace();
    	}
  		return finished ? 0 : 1;
  		
  	}

  	public static class MapWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
	  	private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
	  	private static final Pattern ALPHA_NUMERIC = Pattern.compile("[^ a-zA-Z0-9]");
	  	@Override
	  	public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
	  		String line = value.toString().toLowerCase();
	  		String words[] = WORD_BOUNDARY.split(line);
	  		for(String word : words){
	  			if(!ALPHA_NUMERIC.matcher(word).find() && !word.isEmpty())
	  				context.write(new Text(word), new IntWritable(1));
	  		}
	  	}
  	}
  	
	public static class MapStopWords extends Mapper< Text, Text, IntWritable, Text> {
		@Override
		public void map(Text key, Text value, Context context)throws IOException, InterruptedException {
			context.write(new IntWritable(Integer.parseInt(value.toString())), key);
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
  	
  	public static class ReduceTopKStopWords extends Reducer<IntWritable, Text, IntWritable, Text> {
  		//private Map<IntWritable, Iterable<Text>> countMap = new HashMap<>();
  		//public static final Log log = LogFactory.getLog(ReduceTopKStopWords.class);
  		//static Logger log = Logger.getLogger(log4jExample.class.getName());
  		int i=0;
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//countMap.put(key, values);
			for(Text value :  values){
				if(i<10)
				context.write(key, value);i++;
			}		
        }
		
        /*@Override
        protected void cleanup(Context context) throws IOException, InterruptedException {


            int counter = 0;
            for (IntWritable key: countMap.keySet()) {
                if (counter ++ == 10) {
                    break;
                }
                for(Text value :  countMap.get(key)){
                	//System.out.println(value + " " + key);
                	context.write(key, value);
                }
            }
      	}*/
  	}

}