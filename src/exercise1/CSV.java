package exercise1;


import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
/**
 * Class to generate the files users.csv and transactions.csv.
 * @author Iraklis Bekiaris
 *
 */
public class CSV {
	
	private final String COMMA_DELIMITER = ",";
	private final String NEW_LINE_SEPERATOR = "\n";
	private final String FILE_HEADER = "word, frequency";
	private FileWriter fileWriter = null;
	
	public CSV(){
		try {
			this.fileWriter = new FileWriter("/stopwords.csv");
			fileWriter.append(FILE_HEADER.toString());
			fileWriter.append(NEW_LINE_SEPERATOR);
			
		} catch (Exception e){
			System.out.println("error" + "   " + e.getMessage());
		} 
	}


	
	public void addStopWordToCSV(String word, Integer freq){
		try {
			fileWriter.append(word); 
			fileWriter.append(COMMA_DELIMITER);
			fileWriter.append(String.valueOf(freq));
			fileWriter.append(NEW_LINE_SEPERATOR);			
		} catch (Exception e){
			System.out.println("errorgedrfdfgdf" + "   " + e.getMessage());
		}
	}
	
	public void closeCSV() throws IOException{
		fileWriter.flush();
		fileWriter.close();
	}
	public void print(HashMap<String, Integer> map){
		for(String k : map.keySet()){
			System.out.println(k + " " + map.get(k));
		}
	}
}
