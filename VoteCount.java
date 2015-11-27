/**
 * @author Sharayu
 * Calculate votes in unequal democracy based on worth 
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class VoteCount {

	public static class MergeFilesMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private Text valWord = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			System.out.println("isitlinewise: " + value.toString());
			StringTokenizer itr = new StringTokenizer(value.toString());
			String voter = itr.nextToken();

			String voterval = itr.nextToken().toUpperCase();

			word.set(voter);
			valWord.set(voterval);
			context.write(word, valWord);

		}
	}

	public static class CalculateVotesMapper extends Mapper<Object, Text, Text, IntWritable> {

		private Text word = new Text();
		private IntWritable wordWorth = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			System.out.println("isitlinewise2: " + value.toString());

			StringTokenizer itr = new StringTokenizer(value.toString());
			System.out.println(value);

			// 1st token is the key, you don't want to process it
			itr.nextToken();

			int worth = 0;
			List<String> valList = new ArrayList<String>();
			System.out.println("map1----" + key);
			while (itr.hasMoreTokens()) {
				String val = itr.nextToken();
				System.out.println("map1 vals1: " + val.toString());
				try {
					worth = Integer.parseInt(val.toString());
					System.out.println("map1 worth: " + worth);
				} catch (NumberFormatException e) {
					valList.add(val.toString());
				}
			}
			for (String val : valList) {
				System.out.println("map1 vals2: " + val.toString() + " " + worth);
				word.set(val);
				wordWorth.set(worth);
				context.write(word, wordWorth);
			}
		}
	}

	public static class MergedFilesReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String avals = "";
			for (Text val : values) {
				avals = avals + " " + val.toString();
			}
			result.set(avals);
			context.write(key, result);
		}
	}

	public static class CalculateVotesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int votesum = 0;
			for (IntWritable val : values) {
				votesum += val.get();
			}
			result.set(votesum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job1 = new Job(conf, "combine votes");
		job1.setJarByClass(VoteCount.class);
		job1.setMapperClass(MergeFilesMapper.class);
		job1.setCombinerClass(MergedFilesReducer.class);
		job1.setReducerClass(MergedFilesReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "-tmp"));

		job1.waitForCompletion(true);

		Job job2 = new Job(conf, "votes count");
		job2.setJarByClass(VoteCount.class);
		job2.setMapperClass(CalculateVotesMapper.class);
		job2.setCombinerClass(CalculateVotesReducer.class);
		job2.setReducerClass(CalculateVotesReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(args[1] + "-tmp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
