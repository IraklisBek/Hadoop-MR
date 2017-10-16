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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class StopWords extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StopWords.class);
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new StopWords(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "stopwords");
    job.setJarByClass(this.getClass());
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private boolean caseSensitive = false;
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
    private static final Pattern ALPHA_NUMERIC = Pattern.compile("[^a-zA-Z0-9]");

    protected void setup(Context context)throws IOException, InterruptedException {
      Configuration config = context.getConfiguration();
      this.caseSensitive = config.getBoolean("stopwords.case.sensitive", false);
    }

    public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
      String line = lineText.toString();
      if (!caseSensitive) {
        line = line.toLowerCase();
      }
      Text currentWord = new Text();
        for (String word : WORD_BOUNDARY.split(line)) {
          if (word.isEmpty() || ALPHA_NUMERIC.matcher(word).find()) {
            continue;
          }
          currentWord = new Text(word);
          context.write(currentWord,one);
        }   
        
      }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      if(sum>2)
    	  context.write(word, new IntWritable(sum));
    }
  }
}
