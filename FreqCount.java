/**
 * @author Sharayu
 * Count frequency of occurrence of alphabets in text
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FreqCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text word = new Text();
    private IntWritable count = new IntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

    	int[] arr = new int[26];
    	String str = value.toString().toLowerCase();
    	for(int i=0; i<str.length(); i++)
    	{
    		if(str.charAt(i)>96 && str.charAt(i)<123) {
    			arr[str.charAt(i)-97] = arr[str.charAt(i)-97]+1;
    		}
    	}
    	
    	for(int i=0;i<26;i++) {
    		word.set(Character.toString((char)(i+97)));
    		count.set(arr[i]);    		
    		context.write(word, count);
    	}
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "freq count");
    job.setJarByClass(FreqCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
