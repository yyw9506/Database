/* Java imports */
import java.io.IOException;
import java.util.StringTokenizer;

/* Hadoop imports */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* JSON imports */
import org.json.JSONException;
import org.json.JSONObject;

public class AoT_1 {
	
	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text parameter = new Text();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
				JSONObject jsonobj = new JSONObject(value.toString());
				JSONObject sub_jsonobj = jsonobj.getJSONObject("features");
				parameter.set(sub_jsonobj.getString("parameter"));
				context.write(parameter, one);
			}
		}

	public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
				int sum = 0;
				for (IntWritable val : values) {
		          sum += val.get();
		        }
		        result.set(sum);
		        context.write(key, result);
		    }
		}
	
	public static void main(String[] args) throws Exception {
		/*
		 * arg0 input path
		 * arg1 output path
		 */
		
		// check argument number
		if(args.length < 2) {
			System.out.println("Require 2 arguments.");
			System.exit(1);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "AoT_1");
		job.setJarByClass(AoT_1.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}