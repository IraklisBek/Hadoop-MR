package igonre.test;


import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
/**
 * Class to generate the files users.csv and transactions.csv.
 * @author Iraklis Bekiaris
 *
 */
public class GenerateCSV {
	
	private static final String COMMA_DELIMITER = ",";
	private static final String NEW_LINE_SEPERATOR = "\n";
	private static final String FILE_HEADER_USERS = "user id, username";
	private static final String FILE_HEADER_TRANSACTIONS = "transaction id, user id, transaction type";

	public static void generateCSV(FileWriter CSVFile, IntWritable freq, Text word){
		try {
			CSVFile.append(String.valueOf(word)); 
			CSVFile.append(COMMA_DELIMITER);
			CSVFile.append(String.valueOf(freq));
			CSVFile.append(NEW_LINE_SEPERATOR);			
		} catch (Exception e){
			System.out.println("error" + "   " + e.getMessage());
		}
	}
	
}
