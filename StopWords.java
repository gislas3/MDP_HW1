package stop.words;


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






import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
//import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
//import org.apache.hadoop.io.compress.GzipCodec;
//import org.apache.hadoop.io.compress.Lz4Codec;
//import org.apache.hadoop.io.compress.SnappyCodec;
//import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class StopWords extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      int res = ToolRunner.run(new Configuration(), new StopWords(), args);
      
     
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
	  
	//   getConf().setBoolean(Job.MAP_OUTPUT_COMPRESS, true); 
	  // getConf().setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, BZip2Codec.class,
	    //			  CompressionCodec.class); //for setting the compression to true
      Job myjob = Job.getInstance(getConf());
	  
      
      myjob.setJarByClass(StopWords.class);
      myjob.setOutputKeyClass(Text.class);
      myjob.setOutputValueClass(IntWritable.class);

      myjob.setMapperClass(Map.class);
      myjob.setCombinerClass(Reduce.class); //for setting combiner class (same as reducer)
      myjob.setNumReduceTasks(50); //my addition
      myjob.setReducerClass(Reduce.class);
      
      myjob.setInputFormatClass(TextInputFormat.class);
      
      myjob.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(myjob, new Path(args[0])); //user sets input path
      FileOutputFormat.setOutputPath(myjob, new Path(args[1])); //user sets output path
     
      //FileOutputFormat.
      myjob.waitForCompletion(true);
     
     //for writing the stopwords file to the hdfs
     FileSystem fs = FileSystem.get(getConf());  
     RemoteIterator<LocatedFileStatus> direc = fs.listFiles(new Path(args[1]), false); //open the output directory with the reducers' output
   
     
     String output_path = "stopwords.csv"; //name the file
     FSDataOutputStream out = fs.create(new Path(output_path));
     while(direc.hasNext()) {
     Path temp_path = direc.next().getPath();
     
     if(temp_path.toString().contains("part")) { // only open the file if it is a reducer output
     
     BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(temp_path)));
     try {
    	  String line;
    	  line=br.readLine();
    	  while (line != null){
    		 String[] linesplit = line.split("\\s+");
		     String output  = linesplit[0] + ", " + linesplit[1] + "\n";
		     
    	     out.writeBytes(output);//(output);

    	    
    	    line = br.readLine();
    	  }
    	} finally {
    	  
    	  br.close(); // lcose the file
    	}
     }
     
     }
     out.close();
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable ONE = new IntWritable(1);
      private Text word = new Text();

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
            		else if (i == token.length() -1 ){
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
            
            //System.out.println("Token after processing is: " + token);
            if(!token.isEmpty()) {
            	word.set(token);
            	context.write(word, ONE);
            }
         }
      }
   }

   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
     
	   @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : values) {
            sum += val.get(); //sum up the count
         }
         if(sum >= 4000) {
        	 context.write(key, new IntWritable(sum)); //only write the word to the context if it is a stopword as defined
         }
      }
   }
}