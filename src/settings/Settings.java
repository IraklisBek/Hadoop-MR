package settings;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;


public class Settings {
	private String args[];
	private String combiner;
	private String numReducers;
	private String compress;
	private Job job;
	private ArrayList<String> skipFiles;
	private HashSet<String> skipPatterns;
	private String doc_to_count_words;
	
	public Settings(){
		this.skipPatterns = new HashSet<>();
		this.doc_to_count_words="No Document Selected To Count Words";
	}
	
	public Settings(String[] args, Job job){
		this.args = args;
		this.job = job;
		this.combiner="false";
		this.numReducers="1";
		this.compress="false";
		this.skipFiles = new ArrayList<>();
		
	}
	
	public void selectDocToCountWord(String args[]){
		for (int i = 0; i < args.length; i++) {
			if ("-doc_to_count_words".equals(args[i])) {
				i+=1;
				doc_to_count_words=args[i];
			}
		}
	}
	
	public void setCombiner(Class reduce){
		for (int i = 0; i < this.args.length; i++) {
			if ("-combiner".equals(this.args[i])) {
				i+=1;
				if(this.args[i].equals("true")){
					this.combiner = "true";
					this.job.setCombinerClass(reduce);
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
	
	public void setSkipFiles(){
		for (int i = 0; i < this.args.length; i++) {
			if ("-skip".equals(this.args[i])) {
				this.job.getConfiguration().setBoolean(job.getJobName()+".skip.patterns", true);
				i+=1;
				this.job.addCacheFile(new Path(args[i]).toUri());
				
			}
		}
	}
	
	public HashSet<String> getSkipPatterns(){
		return this.skipPatterns;
	}
	
	public void parse(Configuration conf, Mapper.Context context) throws IOException{
		if (conf.getBoolean(context.getJobName()+".skip.patterns", false)) {
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
				this.skipPatterns.add(pattern.split("\\,")[0]);
			}
		} catch (IOException ioe) {
			System.err.println("Caught exception while parsing the cached file '" 
							+ patternsURI + "' : " + StringUtils.stringifyException(ioe));
		}
	}
	
	public void createFile(String name, String Data) throws IOException{
		Path newFolderPath= new Path(name);

		FileSystem hdfs =FileSystem.get(new Configuration());
		Path homeDir=hdfs.getHomeDirectory();

		if(hdfs.exists(newFolderPath))
		{
			hdfs.delete(newFolderPath, true);
		}

		hdfs.mkdirs(newFolderPath);

		Path newFilePath=new Path(name+"/counters.txt");
		//hdfs.createNewFile(newFilePath);

		StringBuilder sb=new StringBuilder();
		sb.append(Data);
		sb.append("\n");
		byte[] byt=sb.toString().getBytes();
		FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
		fsOutStream.write(byt);
		fsOutStream.close();
	}
	
	@SuppressWarnings("deprecation")
	public void deleteFile(String filePath) throws IOException{
		Path newFolderPath= new Path(filePath);
		FileSystem hdfs =FileSystem.get(new Configuration());
		Path homeDir=hdfs.getHomeDirectory();
		if(hdfs.exists(newFolderPath))
		{
			hdfs.delete(newFolderPath, true);
		}
	}
	
	public String getDocToCountWords(){
		return this.doc_to_count_words;
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
