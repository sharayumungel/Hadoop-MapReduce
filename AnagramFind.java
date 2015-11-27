/**
 * @author Sharayu
 * Find all anagrams in text
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnagramFind {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();
    private Text sortedWord = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
      StringTokenizer itr = new StringTokenizer(value.toString());
      String sortedStr = null;
      while (itr.hasMoreTokens()) {
    	 sortedStr = itr.nextToken();
    	 word.set(sortedStr);
    	 char[] chArr = sortedStr.toLowerCase().toCharArray();
    	 Arrays.sort(chArr);
    	 sortedStr = new String(chArr);
         sortedWord.set(sortedStr);
        context.write(sortedWord, word);
      }
    }
  }

  public static class AnagramReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String avals = "";
      for (Text val : values) {
        avals= avals + " " + val;
      }
      result.set(avals);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "anagram count");
    job.setJarByClass(AnagramFind.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(AnagramReducer.class);
    job.setReducerClass(AnagramReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
