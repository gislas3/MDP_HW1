package inverted.index.wc;


//import java.io.BufferedInputStream;
//import java.io.BufferedOutputStream;
import java.io.BufferedReader;
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileOutputStream;
import java.io.IOException;
//import java.io.InputStream;
import java.io.InputStreamReader;
//import java.io.OutputStream;
//import java.lang.Character;
//import java.nio.charset.Charset;
//import java.nio.file.Files;
//import java.nio.file.OpenOption;
//import java.util.Arrays;

import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.RemoteIterator;
//import org.apache.hadoop.io.IOUtils;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.compress.CompressionCodec;
//import org.apache.hadoop.io.compress.GzipCodec;

//import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndexWC extends Configured implements Tool {
	//private static int distinct_words = 0;
	//protected static LinkedList<String> stopwords = new LinkedList<String>();
 public static void main(String[] args) throws Exception {
    //System.out.println(Arrays.toString(args));
    int res = ToolRunner.run(new Configuration(), new InvertedIndexWC(), args);
    
   
    //System.out.println(distinct_words);
    System.exit(res);
 }

 @Override
 public int run(String[] args) throws Exception {
	 
    Job myjob = Job.getInstance(getConf());
    

    
    myjob.setJarByClass(InvertedIndexWC.class);
    myjob.setOutputKeyClass(Text.class);
    myjob.setOutputValueClass(Text.class);

    myjob.setMapperClass(Map.class);
    myjob.setCombinerClass(Reduce.class); //for setting combiner class
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
 
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    //private final static IntWritable ONE = new IntWritable(1);
    private Text word = new Text();
    
    private Text docname = new Text();
    private LinkedList<String> swords;
    
    
 
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	 FileSystem fs = FileSystem.get(context.getConfiguration());  
    	 swords = new LinkedList<String>(); 
    	    String sw = fs.getHomeDirectory().toString() + "/stopwords.csv";
    	   // System.out.println("Path in Mapper class is: " + sw);
    	   try{
    	    
    	    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(sw))));
    	    try {
    	   	  String line;
    	   	  
    	   	  line=br.readLine();
    	   	
    	   	  while (line != null){
    	   		//System.out.println(line);
    	   		 String[] linesplit = line.split(",");
    	   	     //System.out.println(linesplit[0].substring(0, linesplit[0].length() -1));
    			 //swords.add(linesplit[0].substring(0, linesplit[0].length() -1));
    	   		 //System.out.println(linesplit[0]);
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
          	String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
          	docname.set(fileName  +"#1");
          	context.write(word, docname);
          }
       }
    }

	
 }
/*
 public static class Combine extends Reducer<Text, Text, Text, Text> {
	   @Override
	    public void reduce(Text key, Iterable<Text> values, Context context)
	            throws IOException, InterruptedException {
	       LinkedList<String> filenames = new LinkedList<String>();
	       LinkedList<Integer> namecount = new LinkedList<Integer>();
	       //int x = 0;
	       //System.out.println("key is: " + key.toString());
	       
	       for (Text t: values) {
	          if(!filenames.contains(t.toString())) {
	        	  filenames.add(t.toString());
	        	  namecount.add(1);
	          }
	          else {
	        	  int index = filenames.indexOf(t.toString());
	        	  namecount.set(index, namecount.get(index) + 1);
	          }
	          
	          //System.out.println("t is: " + t.toString() );
	          //x++;
	       }
	       //System.out.println();
	       //System.out.println();
	       /*
	     //  if(x > 1) {
	    	//   System.out.println("key is " + key.toString());
	    	  // System.out.print("values are: ");
	    	   //for(Text k: values) {
	    		//   System.out.print(k.toString() + ", ");
	    	//   }
	    	   
	    	  // System.out.println("filenames are: " +filenames.toString());
	       //}
	         
	        
	       String totfiles = "";
	       for(int i =0; i < filenames.size(); i++) {
	    	   totfiles = totfiles + filenames.get(i) + "#" + namecount.get(i) + "\t";
	       }
	       
	       //totfiles = totfiles.substring(0, totfiles.length()-2);
	       context.write(key, new Text(totfiles));
	      	 //distinct_words += 1;
	    }
 }*/
 
 public static class Reduce extends Reducer<Text, Text, Text, Text> {
   
	   @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
       LinkedList<String> filenames = new LinkedList<String>();
       LinkedList<Integer> namecount = new LinkedList<Integer>();
       //int x = 0;
       //System.out.println("key is: " + key.toString());
       
       for (Text t: values) {
    	  String[] line = t.toString().split("\\s+|,");
    	  for(int i = 0; i < line.length; i++) {
    		  String word = line[i].substring(0, line[i].lastIndexOf('#')); //get the filename
    		  Integer count =  Integer.valueOf(line[i].substring(line[i].lastIndexOf('#')+1, line[i].length())); //get the count, value from # to end of value
    		  if(!filenames.contains(word)) { //first time seeing the file name
    			  filenames.add(word);
    			  namecount.add(count);
    		  }
    		  else {
    			  int index = filenames.indexOf(word);
    			  namecount.set(index, namecount.get(index) + count);
    		  }
    	  }
          //System.out.println("t is: " + t.toString() );
       }
    
        
       String totfiles = "";
       for(int i =0; i < filenames.size(); i++) {
    	   totfiles = totfiles + filenames.get(i) + "#" + namecount.get(i) + ", ";
       }
       
       totfiles = totfiles.substring(0, totfiles.length()-2);
       context.write(key, new Text(totfiles));
      	 //distinct_words += 1;
    }
 }
}