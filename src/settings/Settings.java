package settings;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import exercise2a.StopWordsPerformance.ReduceStopWords;


public class Settings {
	private String args[];
	private String combiner;
	private String numReducers;
	private String compress;
	private Job job;
	private ArrayList<String> skipFiles;
	
	public Settings(String[] args, Job job){
		this.args = args;
		this.job = job;
		this.combiner="false";
		this.numReducers="1";
		this.compress="false";
		skipFiles = new ArrayList<>();
	}
	
	public void setCombiner(){
		for (int i = 0; i < this.args.length; i++) {
			if ("-combiner".equals(this.args[i])) {
				i+=1;
				if(this.args[i].equals("true")){
					this.combiner = "true";
					this.job.setCombinerClass(ReduceStopWords.class);
				}
			}
		}
	}
	
	public void setNumReducers(){
		for (int i = 0; i < this.args.length; i++) {
			if ("-numReducers".equals(this.args[i])) {
				i+=1;
				this.numReducers = this.args[i];
				this.job.setNumReduceTasks(Integer.parseInt(this.numReducers));
			}
		}
	}
	
	public void setCompress(Configuration conf){
		for (int i = 0; i < this.args.length; i++) {
			if ("-compress".equals(this.args[i])) {
				i+=1;
				if(this.args[i].equals("true")){
					this.compress = "true";
		  	  		conf.set("mapreduce.map.output.compress", this.compress);
		  	  		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec"); 
				}
			}
		}
	}
	
	public void setSkipFiles(Job job){
		for (int i = 0; i < this.args.length; i++) {
			if ("-skip".equals(this.args[i])) {
				job.getConfiguration().setBoolean(job.getJobName()+".skip.patterns", true);
				i+=1;
				job.addCacheFile(new Path(args[i]).toUri());
				
			}
		}
	}
	
	public String getCombiner(){
		return this.combiner;
	}
	
	public String getNumReducers(){
		return this.numReducers;
	}
	
	public String getCompress(){
		return this.compress;
	}
	
	public ArrayList<String> getSkipFiles(){
		return this.skipFiles;
	}
}
