package exercise2b;

import java.util.HashSet;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import exercise2a.StopWordsPerformance;
import exercise3.InvertedIndexExtention.Combiner;
import settings.Settings;


public class InvertedIndex extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(StopWordsPerformance.class);	
	
	public static enum COUNTERS{
		DOC_WORDS
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new InvertedIndex(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Settings settings;
		Configuration conf = getConf();
		settings = new Settings();
		settings.selectDocToCountWord(args);
		conf.set("doc_to_count_words", settings.getDocToCountWords());
		Job job = Job.getInstance(conf, "inverted_index");
		settings = new Settings(args, job);
		settings.setSkipFiles();
		settings.setCombiner(Reduce.class);
		settings.setNumReducers();
		settings.setCompress(job);
		settings.deleteFile(args[1]);
		
		
		System.out.println();
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);	
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleOutputs.addNamedOutput(job, "counter", TextOutputFormat.class, Text.class, Text.class);
		
		boolean finished = job.waitForCompletion(true);
		job.waitForCompletion(true);
		Counters counters = job.getCounters();
		Counter doc_words = counters.findCounter(COUNTERS.DOC_WORDS);
		System.out.println(doc_words.getValue() + " " + conf.get("doc_to_count_words"));
		settings.createFile("/counter/counters.txt", doc_words.getValue() + " words in file " + conf.get("doc_to_count_words"));												
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

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private String doc_to_count_words;
		private int i;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			doc_to_count_words = conf.get("doc_to_count_words").toString();
			i=0;
		}
		@Override
		public void reduce(Text word, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			boolean first = true;
			StringBuilder docs = new StringBuilder();
			for(Text value : values){
				if (!first){
					docs.append(", ");
				}
				first=false;
				docs.append(value.toString());
				if(value.toString().equals(doc_to_count_words)){
					context.getCounter(COUNTERS.DOC_WORDS).increment(1);
				}
			}
			i++;
			context.write(new Text(String.valueOf(i) + " " + word), new Text(docs.toString()));
		}
	}
}

