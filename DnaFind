/**
 * @author Sharayu
 * Find all people having same dna sequence
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DnaFind {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private Text sortedWord = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			System.out.println("isitlinewise: " + value.toString());
			StringTokenizer itr = new StringTokenizer(value.toString());
			String user = itr.nextToken();
			String sortedStr = itr.nextToken();

			word.set(user);

			sortedWord.set(sortedStr.toUpperCase());
			context.write(sortedWord, word);

		}
	}

	public static class DnaReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String avals = "";
			for (Text val : values) {
				avals = avals + " " + val;
			}
			result.set(avals);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "dna count");
		job.setJarByClass(DnaFind.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(DnaReducer.class);
		job.setReducerClass(DnaReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
