/* Java imports */
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.lang.Iterable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;

/* Spark imports */
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

/* JSON imports */
import org.json.JSONException;
import org.json.JSONObject;

public class SparkAoT_2 {
	
	public static void main(String[] args) {
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
		
		//check operator type
		List<String> operations = new ArrayList<String>();
		operations.add("avg");
		operations.add("min");
		operations.add("max");

		if(!operations.contains(args[4])) {
			System.out.println("Only support avg, min, max.");
			System.exit(1);
		}
		
		String input_path = args[0];
		String startdate_str = args[1];
		String enddate_str = args[2];
		String parameter_type = args[3];
		String operation_type = args[4];
		String output_path = args[5];
		
		// essential to run any spark code
		SparkConf conf = new SparkConf().setAppName("SparkAoT_2");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// load input data to RDD
		JavaRDD<String> dataRDD = sc.textFile(input_path);
		
		@SuppressWarnings("serial")
		JavaPairRDD<String, Integer> result =
			    dataRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>(){
				    public Iterator<Tuple2<String, Integer>> call(String value){
				    	
						JSONObject jsonobj = new JSONObject(value.toString());
						JSONObject sub_jsonobj = jsonobj.getJSONObject("features");
						
						// parameter
						List<Tuple2<String, Integer>> parameter =
						     	new ArrayList<Tuple2<String, Integer>>();
						
						// get record date
						Long dateinmillsecs = (Long) jsonobj.get("timestamp");
						Date date = new Date(dateinmillsecs);
						
						// get start date and end date
						SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
						Date startdate = null;
						Date enddate = null;
						try {
							startdate = dateformat.parse(startdate_str);
							enddate = dateformat.parse(enddate_str);
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
						// match date interval
						if (startdate.getTime() <= date.getTime() && date.getTime() <= enddate.getTime()) {
							
							// match parameter type
							if(sub_jsonobj.getString("parameter").equals(parameter_type)) {
								
								parameter.add(new Tuple2<String, Integer>(sub_jsonobj.getString("parameter"), sub_jsonobj.getInt("value_hrf")));
								
							}
						}
						
						return parameter.iterator();
				    }
				});
		
		int sum = 0;
		int avg = 0;
		int min_of_values = Integer.MAX_VALUE;
		int max_of_values = Integer.MIN_VALUE;
		int parameter_value = 0;
		
		// convert RDD to Collection
		List<Tuple2<String, Integer>> result_list = result.collect();
		System.out.println("Record number: " + result_list.size());
		
		for(Tuple2<String, Integer> tup: result_list) {
			sum += tup._2;
			min_of_values = Math.min(min_of_values, tup._2);
			max_of_values = Math.max(max_of_values, tup._2);
		}
		
		if (result_list.size() > 0) {
			avg = sum / result_list.size();
		}else {
			System.out.println("No record found for given parameter, start date, and enddate.");
			System.exit(1);
		}
		
		// check operation type
		if(operation_type.equals("min")) {
			System.out.println("Max value of parameter: " + parameter_type + "  " + min_of_values);
			parameter_value = min_of_values;
		}else if (operation_type.equals("max")) {
			System.out.println("Max value of parameter: " + parameter_type + "  " + max_of_values);
			parameter_value = max_of_values;
		}else {
			System.out.println("Average value of parameter: " + parameter_type + "  " + avg);
			parameter_value = avg;
		}
		
		// save output data to disk
		List<Tuple2<String, Integer>> final_data = Arrays.asList(new Tuple2<String, Integer>(parameter_type, parameter_value));
		JavaRDD<Tuple2<String, Integer>> final_result = sc.parallelize(final_data);
		final_result.saveAsTextFile(output_path);		
	}
}