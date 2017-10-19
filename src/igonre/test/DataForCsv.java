package igonre.test;

public class DataForCsv {
	private int freq;
	private char word;
	/**
	 * Constructor
	 * @param id		the user id.
	 * @param username	the user name.
	 */
	public DataForCsv(int freq, char word){
		this.freq = freq;
		this.word = word;
	}
	/**
	 * 
	 * @return	the user id.
	 */
	public int getFreq(){
		return freq;
	}
	/**
	 * 
	 * @return	the user name.
	 */
	public char getWord(){
		return word;
	}
}
