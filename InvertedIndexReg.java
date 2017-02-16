package inverted.index.reg;



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
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
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndexReg extends Configured implements Tool{
	//private static int distinct_words = 0;
	//protected static LinkedList<String> stopwords = new LinkedList<String>();
 public static void main(String[] args) throws Exception {
    //System.out.println(Arrays.toString(args));
    int res = ToolRunner.run(new Configuration(), new InvertedIndexReg(), args);
    
   
    //System.out.println(distinct_words);
    System.exit(res);
 }

 @Override
 public int run(String[] args) throws Exception {
	 
    Job myjob = Job.getInstance(getConf());
    

    
    myjob.setJarByClass(InvertedIndexReg.class);
    myjob.setOutputKeyClass(Text.class);
    myjob.setOutputValueClass(Text.class);

    myjob.setMapperClass(Map.class);
    //myjob.setCombinerClass(Combine.class); //for setting combiner class
    myjob.setNumReduceTasks(1); //my addition
    myjob.setReducerClass(Reduce.class);
    
    myjob.setInputFormatClass(TextInputFormat.class);
    
    myjob.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(myjob, new Path(args[0]));
    FileOutputFormat.setOutputPath(myjob, new Path(args[1]));
   
    //FileOutputFormat.
    myjob.waitForCompletion(true);
   

    return 0;
 }
 
 //key will be a text, as will a value be
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    //private final static IntWritable ONE = new IntWritable(1);
    private Text word = new Text();
    
    private Text docname = new Text();
    private HashSet<String> swords;
    
    
 
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	 FileSystem fs = FileSystem.get(context.getConfiguration());  
    	 swords = new HashSet<String>(); 
    	    String sw = fs.getHomeDirectory().toString() + "/stopwords.csv";
    	   // System.out.println("Path in Mapper class is: " + sw);
    	   try{
    	    
    	    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(sw))));
    	    try {
    	   	  String line;
    	   	  
    	   	  line=br.readLine();
    	   	
    	   	  while (line != null){
    	   		 String[] linesplit = line.split(",");
    	   		 swords.add(linesplit[0]);
    			//System.out.println(linesplit[0]);
    			 
    			 line = br.readLine();
    	   	  }
    	   	} finally {
    	   	  
    	   	  br.close();
    	   	}
    	    }catch(IOException e) {
    	    	System.out.println(e.toString());
    	    }	  
    }
    
    
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
  	  
    	
       for (String token: value.toString().split("\\s+|--")) {
          token = token.toLowerCase();
          int end_index = token.length();
          int i = 0;
          //System.out.println("Token prior to processing is: " + token);
          while(i < end_index) {
          	//
          	Character c = token.charAt(i);
          	
          	if(!Character.isLetterOrDigit(c)) { // not a letter or number
          		if(i == 0) { //first character in string
          			//if(c != '\'' || token.charAt(token.length()-1) == '\'') { //first letter is apostrophe, but not being used as quote (for Mark Twain's slang)
          			token = token.substring(1, token.length());
          			i--;
          			//}
          		}
          		else if (i == token.length() -1 ) {
          			token = token.substring(0, token.length()-1);
          		}
          		else {
          			if(c != '-' && c != '\'') {
          				token = token.substring(0, i) + token.substring(i+1, token.length());
          				i--;
          			}	
          		}
          		
          	}
          	i++;
          	end_index = token.length();
          }
          
          if(swords == null) {
        	  System.out.println("Stop words is empty!");
        	  System.exit(1);
          }
          //System.out.println("Token after processing is: " + token);
          if(!token.isEmpty() && !swords.contains(token)) {
          	word.set(token);
          	String fileName = ((FileSplit) context.getInputSplit()).getPath().getName(); //value will be filename
          	docname.set(fileName);
          	context.write(word, docname);
          }
       }
    }

	
 }


 
 public static class Reduce extends Reducer<Text, Text, Text, Text> {
   
	   @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
       LinkedList<String> filenames = new LinkedList<String>();
       //int x = 0;
       //System.out.println("key is: " + key.toString());
       String totfiles = "";
       for (Text t: values) {
    	 
    	  if(!filenames.contains(t.toString())) {
    		  totfiles = totfiles + t.toString() + ", ";
    		  filenames.add(t.toString());
    	  }
          //System.out.println("t is: " + t.toString() );
          //x++;
       }

        
       
       
       totfiles = totfiles.substring(0, totfiles.length()-2);
       context.write(key, new Text(totfiles));
      	 //distinct_words += 1;
    }
 }
}
