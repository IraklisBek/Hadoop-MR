package exercise3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import exercise2a.StopWordsPerformance;
import settings.Settings;


public class InvertedIndexExtention extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(StopWordsPerformance.class);	

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new InvertedIndexExtention(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Settings settings;
		Configuration conf = getConf();
		settings = new Settings();
		settings.selectDocToCountWord(args);
		Job job = Job.getInstance(conf, "inverted_index");
		settings = new Settings(args, job);
		settings.setSkipFiles();
		settings.setCombiner(Combiner.class);
		settings.setNumReducers();
		settings.setCompress(conf);
		settings.deleteFile(args[1]);


		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(Reduce.class);	
		job.setNumReduceTasks(2);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);		

		boolean finished = job.waitForCompletion(true);
		job.waitForCompletion(true);											
		return finished ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Set<String> patternsToSkip = new HashSet<String>();
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		private static final Pattern ALPHA_NUMERIC = Pattern.compile("[^ a-zA-Z0-9]");
		private Text location = new Text();
		public static Settings settings;

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			settings = new Settings();
			settings.parse(conf, context);
			patternsToSkip = settings.getSkipPatterns();
		}

		public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			String line = value.toString().toLowerCase();
			for (String word : WORD_BOUNDARY.split(line)) {
				if (!word.isEmpty() && !patternsToSkip.contains(word) && !ALPHA_NUMERIC.matcher(word).find()) {		
					location.set(fileName);
					context.write(new Text(word),location);
				}
			}             
		}
	}

	public static class Combiner extends Reducer<Text, Text, Text, Text> {
		HashMap<Text, HashMap<Text, Integer>> wordDocsFreq;
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			wordDocsFreq = new HashMap<>();
		}
		@Override
		public void reduce(Text word, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			for (Text value : values) {
				if(wordDocsFreq.containsKey(word)){
					HashMap<Text, Integer> docsFreq = wordDocsFreq.get(word);
					for(Text doc : docsFreq.keySet()){
						if(docsFreq.containsKey(value)){
							int freq = docsFreq.get(doc);
							freq++;
							docsFreq.put(doc, freq);
						}else{
							docsFreq.put(doc, 1);
						}
					}

				}else{
					HashMap<Text, Integer> map2 = new HashMap<>();
					map2.put(value, 1);
					wordDocsFreq.put(word, map2);					
				}				
			}
			HashMap<Text, Integer> docsFreq = wordDocsFreq.get(word);
			for(Text doc : docsFreq.keySet()){
				context.write(new Text(word), new Text(doc + "," + String.valueOf(docsFreq.get(doc))));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private int i;
		HashMap<String, String> wordDocsFreq;
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			i=0;
			wordDocsFreq = new HashMap<>();
		}
		public void reduce(Text word, Iterable<Text> values, Context context)throws IOException, InterruptedException {

			for(Text val : values){
				String []valTokens = val.toString().split(",");
				if(wordDocsFreq.containsKey(word.toString())){
					String str = wordDocsFreq.get(word.toString());
					str += ","+valTokens[0]+"#"+valTokens[1];
					wordDocsFreq.put(word.toString(), str);
				}else{
					wordDocsFreq.put(word.toString(), valTokens[0]+"#"+valTokens[1]);
				}
			}
		}
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for(String w : wordDocsFreq.keySet()){
				context.write(new Text(String.valueOf(i) + " " + w), new Text(wordDocsFreq.get(w)));
				i++;
			}
		}
	}
}

