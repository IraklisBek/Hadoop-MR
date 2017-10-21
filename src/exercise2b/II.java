package exercise2b;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class II {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	  	private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
	  	private static final Pattern ALPHA_NUMERIC = Pattern.compile("[^ a-zA-Z0-9]");
		private Text word = new Text();
		private Text location = new Text();	
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	  		String line = value.toString().toLowerCase();
	  		String words[] = WORD_BOUNDARY.split(line);
			FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			location.set(fileName);
	  		for(String word : words){
	  			this.word.set(word);
	  			if(!ALPHA_NUMERIC.matcher(word).find() && !word.isEmpty())
	  				output.collect(this.word, location);
	  		}			
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			boolean first = true;
			StringBuilder toReturn = new StringBuilder();
			while (values.hasNext()) {
				if (!first)
					toReturn.append(", ");
				first=false;
				toReturn.append(values.next().toString());
			}
			output.collect(key, new Text(toReturn.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(InvertedIndex.class);
		conf.setJobName("InvertedIndex");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
