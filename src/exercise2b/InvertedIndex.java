package exercise2b;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class InvertedIndex extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(InvertedIndex.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new InvertedIndex(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "wordcount");
		for (int i = 0; i < args.length; i++) {
			if ("-skip".equals(args[i])) {
				job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
				i+=1;
				job.addCacheFile(new Path(args[i]).toUri());
				
			}
		}
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private boolean caseSensitive = false;
		private Set<String> patternsToSkip = new HashSet<String>();
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		private static final Pattern ALPHA_NUMERIC = Pattern.compile("[^ a-zA-Z0-9]");
		private Text location = new Text();

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
			if (config.getBoolean("wordcount.skip.patterns", false)) {
				URI[] localPaths = context.getCacheFiles();
				parseSkipFile(localPaths[0]);
			}
		}

		private void parseSkipFile(URI patternsURI) {
			try {
				@SuppressWarnings("resource")
				BufferedReader br = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
				String pattern;
				while ((pattern = br.readLine()) != null) {
					patternsToSkip.add(pattern.split("\\,")[0]);
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file '" 
								+ patternsURI + "' : " + StringUtils.stringifyException(ioe));
			}
		}

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			String line = lineText.toString();
			if (!caseSensitive) {
				line = line.toLowerCase();
			}
			Text currentWord = new Text();
			for (String word : WORD_BOUNDARY.split(line)) {
				if (!word.isEmpty() && !patternsToSkip.contains(word) && !ALPHA_NUMERIC.matcher(word).find()) {		
					currentWord = new Text(word);
					location.set(fileName);
					context.write(currentWord,location);
				}
			}             
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			boolean first = true;
			StringBuilder docs = new StringBuilder();
			for(Text value : values){
				if (!first)
					docs.append(", ");
				first=false;
				docs.append(value.toString());
			}
			context.write(word, new Text(docs.toString()));
		}
	}
}

