/* Java imports */
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

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

public class AoT_2 {
	
	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{
	
		
		// k, v
		private final static IntWritable num = new IntWritable(1);
		private Text parameter = new Text();
	
		private Date startdate;
		private Date enddate;
		private String parameter_type = "";
		private String operator_type = "";
		private SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
				Configuration conf = context.getConfiguration();
				
				JSONObject jsonobj = new JSONObject(value.toString());
				JSONObject sub_jsonobj = jsonobj.getJSONObject("features");
				
				Long dateinmillsecs = (Long) jsonobj.get("timestamp");
				Date date = new Date(dateinmillsecs);
				
				// get start date and end date
				try {
					startdate = dateformat.parse(conf.get("StartDate"));
					enddate = dateformat.parse(conf.get("EndDate"));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				// match date interval
				if (startdate.getTime() <= date.getTime() && date.getTime() <= enddate.getTime()) {
					
					// get parameter type from configuration
					parameter_type =  conf.get("ParameterType");
					
					// match parameter type
					if (sub_jsonobj.getString("parameter").equals(parameter_type)) {
						
						num.set(sub_jsonobj.getInt("value_hrf"));
						parameter.set(parameter_type);
						context.write(parameter, num);

					}
				}
			}
		}

	public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		
		// k, v
		private Text parameter = new Text();
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			
				Configuration conf = context.getConfiguration();
				String operation = conf.get("OperationType");
				
				int sum = 0;
				int counter = 0;
				int min_of_values = Integer.MAX_VALUE;
				int max_of_values = Integer.MIN_VALUE;
						
				for (IntWritable val : values) {
					sum += val.get();
					counter += 1;
					min_of_values = Math.min(min_of_values, val.get());
					max_of_values = Math.max(max_of_values, val.get());
		        }
				
				parameter.set(key.toString()); 
				
				// check operation type
				if(operation.equals("min")) {
					result.set(min_of_values);
				}else if(operation.equals("max")) {
					result.set(max_of_values);
				}else {
					result.set(sum/counter);
				}
		        context.write(parameter, result);
		    }
		}
	
	public static void main(String[] args) throws Exception {
		/*
		 * arg0 input path
		 * arg1 start date
		 * arg2 end date
		 * arg3 name of parameter
		 * arg4 aggregate operator
		 * arg5 output path
		 */
		
		// check argument number
		if(args.length < 6) {
			System.out.println("Require 6 arguments.");
			System.exit(1);
		}
		
		// check date input format
		try {
			SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
			Date startdate = dateformat.parse(args[1]);
			Date enddate = dateformat.parse(args[2]);
		}catch (ParseException e) {
			// TODO Auto-generated catch block
			System.out.println("Date format is wrong, please use this format: yyyy-MM-dd.");
			System.exit(1);
		}
		
		// check operator type
		List<String> operations = new ArrayList<String>();
		operations.add("avg");
		operations.add("min");
		operations.add("max");

		if(!operations.contains(args[4])) {
			System.out.println("Only support avg, min, max.");
			System.exit(1);
		}
		
		// global configuration 
		Configuration conf = new Configuration();
		
		// arg1-4
		conf.set("StartDate", args[1]);
		conf.set("EndDate", args[2]);
		conf.set("ParameterType", args[3]);
		conf.set("OperationType", args[4]);
		
		// job configuration
		Job job = Job.getInstance(conf, "AoT_2");
		job.setJarByClass(AoT_2.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// arg0, arg5
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[5]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}