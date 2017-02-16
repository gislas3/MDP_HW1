package relative.frequencies;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class RelativeFrequencyPairs extends Configured implements Tool {
	//private static int distinct_words = 0;
	   public static void main(String[] args) throws Exception {
	      //System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new RelativeFrequencyPairs(), args); 
	      
	     
	      //System.out.println(distinct_words);
	      System.exit(res);
	   }

	   @Override
	   public int run(String[] args) throws Exception {
	      
	      Job myjob = Job.getInstance(getConf()); // initialize the job
	      myjob.setJarByClass(RelativeFrequencyPairs.class); //set the Jar to this class
	      myjob.setOutputKeyClass(Text.class); // word will be the key, so key class should be a text
	      myjob.setOutputValueClass(DoubleWritable.class); // pair approach, so output a double as the value
	      
	      myjob.setMapperClass(MapPairs.class); // Map class is MapPairs.class
	      myjob.setNumReduceTasks(1); //use only one reducer since want sorted output and only want 100 output
	      myjob.setCombinerClass(CombinePairs.class);
	      myjob.setReducerClass(ReducePairs.class); // set the reducer class to ReducePairs
	      myjob.setInputFormatClass(TextInputFormat.class); //Text input format, reading in files
	      
	      myjob.setOutputFormatClass(TextOutputFormat.class); //writing out text, writing out files

	      FileInputFormat.addInputPath(myjob, new Path(args[0])); // user specified input path
	      FileOutputFormat.setOutputPath(myjob, new Path(args[1])); // user specified output path
	     
	      myjob.waitForCompletion(true); 

	   
	     
	     
	      return 0;
	   }
	   
	
	   /*
	    * Map pairs outputs a key value pairing of two words separated by a tab, and one (word1\tword2, 1)
	    */
	   
	   public static class MapPairs extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		   private final static DoubleWritable ONE = new DoubleWritable(1); // what will be written out as the value
		    private LinkedList<String> swords; //will store the stop words
		    
		 
		    @Override //call setup in order to read in the stopwords
		    protected void setup(Context context) throws IOException, InterruptedException {
		    	 FileSystem fs = FileSystem.get(context.getConfiguration());  
		    	 swords = new LinkedList<String>(); //initialize swords (stores the stopwords)
		    	    String sw = fs.getHomeDirectory().toString() + "/stopwords.csv"; // assumes a file named stopwords.csv exists in the home directory
		    	   // System.out.println("Path in Mapper class is: " + sw);
		    	   try{
		    	    
		    	    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(sw)))); // will be used for reading in stopwords
		    	    try {
		    	   	  String line;
		    	   	  
		    	   	  line=br.readLine();
		    	   	
		    	   	  while (line != null){
		    	   		 String[] linesplit = line.split(","); //split along the commas
		    	   	    
		    	   		 swords.add(linesplit[0]); //just add the first part, not the count
		    			//System.out.println(linesplit[0]); ..for debugging
		    			 
		    			 line = br.readLine(); //read the line
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
		  	  
		    	LinkedList<String> linewords = new LinkedList<String>(); //will store the words in a line
		       for (String token: value.toString().split("\\s+|--")) { //split the string along spaces and "--"
		          token = token.toLowerCase(); // turn the string to lowe case
		          int end_index = token.length(); // keep track of the last index of the string
		          int i = 0; // start index
		          //System.out.println("Token prior to processing is: " + token);
		          while(i < end_index) { //loop through the string
		          	//
		          	Character c = token.charAt(i);
		          	
		          	if(!Character.isLetterOrDigit(c)) { // not a letter or number
		          		if(i == 0) { //if first character of string not a letter or number, remove it
		          			//if(c != '\'' || token.charAt(token.length()-1) == '\'') { //first letter is apostrophe, but not being used as quote (for Mark Twain's slang)
		          			token = token.substring(1, token.length()); 
		          			i--;
		          			//}
		          		}
		          		else if (i == token.length() -1 ) { //if non alphanumeric character at end of string, remove it
		          			token = token.substring(0, token.length()-1);
		          		}
		          		else {
		          			if(c != '-' && c != '\'') { //if non alphanumeric character in middle of string, leave it if it is a hyphen or apostrophe
		          				token = token.substring(0, i) + token.substring(i+1, token.length());
		          				i--;
		          			}	
		          		}
		          		
		          	}
		          	i++;
		          	end_index = token.length();
		          }
		          
		          if(swords == null) { //should not reach here if csv file exists
		        	  System.out.println("Stop words is empty!");
		        	  System.exit(1);
		          }
		          //System.out.println("Token after processing is: " + token);
		          if(!token.isEmpty() && !swords.contains(token)) { //check to make sure token wasn't all nonalphanumeric characters or empty
		          	linewords.add(token);
		          	
		          }
		       }
		       
		       for(int i = 0; i < linewords.size(); i++) {
		    	   
		    	   //wordkey.set(linewords.get(i));
		    	   String key1 = linewords.get(i); //first half of the key
		    	   double count = 0;
		    	   for(int j = 0; j < linewords.size(); j++) {
		    		   String key2 = linewords.get(j); 
		    		   if(!key2.equals(key1)) { //if theyre not the same, write them to the same
		    			   context.write(new Text(key1 + "\t" + key2), ONE);
		    			   count++;
		    		   }
		    	   }
		    	   context.write(new Text(key1 + "\t"), new DoubleWritable(count));
		       }
		    }
	   }
	   
	   
	 public static class CombinePairs extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		   @Override
		    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
		            throws IOException, InterruptedException {
				   double d = 0;
				   for(DoubleWritable v : values){
					   d+= v.get(); //just add the values up
				   }
				   context.write(key, new DoubleWritable(d));
		   }
	 }
	   
	   public static class ReducePairs extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		   private double[] maxvals = new double[100]; //the array that will store teh frequencies
		   private String[] finaloutput = new String[100]; // the array that will store the strings that go with the frequencies above
		   private double denom = 0;
		 
		   @Override
	    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
	            throws IOException, InterruptedException {
			   
			   double num = 0;
			   double d2 = 0;
			   String[] keys = key.toString().split("\\s++");
			   for(DoubleWritable v : values) {
			   if(keys.length == 1) { // a denominator term
				   d2 += v.get();
			   	}
			   else { // a numerator term
				   num += v.get();
			   }
			   }
			   if(keys.length == 1) { //set the denominator for this round
				   denom = d2;
			   }
			   double fr = num/denom; // the value of the frequency
			  
				  if(denom > 50 && Double.compare(fr, maxvals[0]) > 0) { //minimum denominator of 50 and use Double.compare to avoid numerical issues
					  int c = 1; //start at 1 because already know it is greater than first element
					  while(c < maxvals.length && Double.compare(fr, maxvals[c]) > 0) { //increase index until reach end or less than current index
						  c++;
					  }
					  c = c-1;
					  double torepd = fr;
					  String toreps = key.toString();
					  for(int i = c; i > -1; i--) { //shift all elements of the array to the left
						   double tempd = maxvals[i];
						   String temps = finaloutput[i];
						   maxvals[i] = torepd;
						   finaloutput[i] = toreps;
						   torepd = tempd;
						   toreps = String.valueOf(temps);
					   }
				  }
			   
			
			   
		   }
		   
		   //cleanup will write the output to the file
		   @Override
		   protected void cleanup(Context context) throws IOException,
           InterruptedException{
			   
			  
			
			   for(int i = finaloutput.length-1; i > -1; i--) { //start at the end since array is sorted ascending
				   String[] outformat = finaloutput[i].split("\\s++");
				   context.write(new Text(outformat[0] + ", " + outformat[1]), new DoubleWritable(maxvals[i])); 
			   }
			
		   }
		   
		}
	   
}
