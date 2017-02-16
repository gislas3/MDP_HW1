package relative.frequencies;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class RelativeFrequencyStripes extends Configured implements Tool {
	   public static void main(String[] args) throws Exception {
	   
	      int res = ToolRunner.run(new Configuration(), new RelativeFrequencyStripes(), args);
	      
	     
	      System.exit(res);
	   }

	   @Override
	   public int run(String[] args) throws Exception {

	      Job myjob = Job.getInstance(getConf()); //initialize the job
	      myjob.setJarByClass(RelativeFrequencyStripes.class); // set the jar file to this class
	      myjob.setOutputKeyClass(Text.class); //output key will be a text
	      myjob.setOutputValueClass(Text.class); //output value will be a text since using stripes approach
	      
	      myjob.setMapperClass(MapStripes.class); //for setting mapper class
	      myjob.setCombinerClass(CombineStripes.class); //for setting combiner class
	      myjob.setNumReduceTasks(1); //use only one reducer since want sorted output and only want top 100
	      myjob.setReducerClass(ReduceStripes.class); //set reducer to ReduceStripes
	      myjob.setInputFormatClass(TextInputFormat.class); //set the input format class to text input (reading from file)
	      
	      myjob.setOutputFormatClass(TextOutputFormat.class); //set output to text since writing to file

	      FileInputFormat.addInputPath(myjob, new Path(args[0])); //user sets input path
	      FileOutputFormat.setOutputPath(myjob, new Path(args[1])); //user sets output path
	     
	      //FileOutputFormat.
	      myjob.waitForCompletion(true);
	     

	   
	     
	   
	      return 0;
	   }
	   
	   /*
	    * Sets Mapper Class - sets keys and values as texts (key will be a word in aline, value will be other words)
	    */
	   public static class MapStripes extends Mapper<LongWritable, Text, Text, Text> {
		    private Text wordkey = new Text(); // will store the key
		    
		    private Text wordvals = new Text(); //will store the values
		    private LinkedList<String> swords; //will store the stopwords
		    
		 
		    @Override
		    protected void setup(Context context) throws IOException, InterruptedException {
		    	 FileSystem fs = FileSystem.get(context.getConfiguration());  
		    	 swords = new LinkedList<String>(); 
		    	    String sw = fs.getHomeDirectory().toString() + "/stopwords.csv"; //assumes the stopwords.csv is stored in the home directory
		    	   // System.out.println("Path in Mapper class is: " + sw);
		    	   try{
		    	    
		    	    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(sw)))); // open the file
		    	    try {
		    	   	  String line;
		    	   	  
		    	   	  line=br.readLine(); 
		    	   	
		    	   	  while (line != null){
		    	   		//System.out.println(line);
		    	   		 String[] linesplit = line.split(","); //split at the comma
		    	   		 swords.add(linesplit[0]); // only add the word, not its count
		    			//System.out.println(linesplit[0]);
		    			 
		    			 line = br.readLine();
		    	   	  }
		    	   	} finally {
		    	   	  
		    	   	  br.close(); //close the file
		    	   	}
		    	    }catch(IOException e) {
		    	    	System.out.println(e.toString());
		    	    }	  
		    }
		    
		    
		    @Override
		    public void map(LongWritable key, Text value, Context context)
		            throws IOException, InterruptedException {
		  	  
		    	LinkedList<String> linewords = new LinkedList<String>();
		       for (String token: value.toString().split("\\s+|--")) {
		          token = token.toLowerCase();
		          int end_index = token.length();
		          int i = 0;
		          //System.out.println("Token prior to processing is: " + token);
		          while(i < end_index) {
		          	//
		          	Character c = token.charAt(i);
		          	
		          	if(!Character.isLetterOrDigit(c)) { // not a letter or number
		          		if(i == 0) { //first character in string (always remove if not alphanumeric
		          			//if(c != '\'' || token.charAt(token.length()-1) == '\'') { //first letter is apostrophe, but not being used as quote (for Mark Twain's slang)
		          			token = token.substring(1, token.length());
		          			i--;
		          			//}
		          		}
		          		else if (i == token.length() -1 ) { // always remove punctuation from end of string
		          			token = token.substring(0, token.length()-1);
		          		}
		          		else {
		          			if(c != '-' && c != '\'') { //remove punctuation in middle of string if not a hyphen or apostrophe
		          				token = token.substring(0, i) + token.substring(i+1, token.length());
		          				i--;
		          			}	
		          		}
		          		
		          	}
		          	i++;
		          	end_index = token.length();
		          }
		          
		          if(swords == null) { //shouldnt reach here - error check
		        	  System.out.println("Stop words is empty!");
		        	  System.exit(1);
		          }
		          //System.out.println("Token after processing is: " + token);
		          if(!token.isEmpty() && !swords.contains(token)) { //add the word if it isnt the empty string or in stopwords
		          	linewords.add(token);
		          	
		          }
		       }
		       
		       for(int i = 0; i < linewords.size(); i++) {
		    	   
		    	   wordkey.set(linewords.get(i)); //set the key
		    	   String vals = ""; 
		    	   for(int j = 0; j < linewords.size(); j++) {
		    		   if(!linewords.get(j).equals(linewords.get(i))) { //create teh string of values - all words in line aside from key word
		    			   vals = vals + linewords.get(j) + "\t";
		    		   }
		    	   }
		    	   if(!vals.isEmpty()) {
		    		   wordvals.set(vals);
		    		   context.write(wordkey, wordvals);
		    	   }
		       }
		    }
	   }
	   
	   
	   public static class CombineStripes extends Reducer<Text, Text, Text, Text> {
		   @Override
		    public void reduce(Text key, Iterable<Text> values, Context context)
		            throws IOException, InterruptedException {
			   String newvals = "";
			   int count = 0;
			   
			   for(Text v : values) {
				   if(count == 0) {
					   newvals = v.toString();
				   }
				   else {
					   newvals = newvals + "\t" + v;
				   }
				   count++;
			   }
			   context.write(key, new Text(newvals));
			   
		   }
		   
	   }
	   
	   
	   /*
	    * ReduceStripes 
	    */
	   public static class ReduceStripes extends Reducer<Text, Text, Text, Text> {
		   private double[] maxvals = new double[100]; //double array will store the pair of values
		   //private int counter = 0;
		   private String[] finaloutput = new String[100]; // string array will store the pair of words
		   private DecimalFormat df = new DecimalFormat("0.00000"); //use decimal format to format the double
		   @Override
	    public void reduce(Text key, Iterable<Text> values, Context context)
	            throws IOException, InterruptedException {
			   int denom = 0; //will store the value of the denominator (aka number of other words)
			   HashMap<String, Integer> newvals = new HashMap<String, Integer>(); //hashes all values to their count (ie keeps track of co-occurrences)
			   for(Text v : values) { 
				   String[] t = v.toString().split("\\s+"); //split along white spaces
				   for(int i = 0; i < t.length; i++) {
					   if(!newvals.containsKey(t[i])) { //havent seen value word before
						   newvals.put(t[i], 1);
					   }
					   else {
						   newvals.put(t[i], 1+ newvals.get(t[i]));  //have seen it before
					   }
					   denom++; //increment the denominator for each word
				   }
			   }
			   
			   for(String k : newvals.keySet()) {
				   double fr = ((double) newvals.get(k))/((double) denom); //calculate the frequency for the word and its pair
				
					  if(denom > 50 && Double.compare(fr, maxvals[0]) > 0) { //only enter if denominator is greater than 50 and frequency is greater than first element of array
						  int c = 1; //start at 1 since already greater than first element
						  while(c < maxvals.length && Double.compare(fr, maxvals[c]) > 0) { //increment the index (attempts to save time by not incrementing each time
							  c++;
						  }
						  c = c-1;
						  double torepd = fr;
						  String toreps = key + ", "  + k;
						  for(int i = c; i > -1; i--) { // shift all elements of array to the left
							   double tempd = maxvals[i];
							   String temps = finaloutput[i];
							   maxvals[i] = torepd;
							   finaloutput[i] = toreps;
							   torepd = tempd;
							   toreps = String.valueOf(temps);
						   }
					  }
			   }
			   
		   }
		   
		   //cleanup prints the top 100 frequencies to the file
		   @Override
		   protected void cleanup(Context context) throws IOException,
           InterruptedException{
			   for(int i = finaloutput.length-1; i > -1; i--) {
				   context.write(new Text(finaloutput[i]), new Text(df.format(maxvals[i])));
			   }
		   }
		   
		}
	   
	
	   
}
