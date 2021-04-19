/* Java imports */
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.lang.Iterable;
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
import org.apache.spark.api.java.function.PairFlatMapFunction;

/* JSON imports */
import org.json.JSONException;
import org.json.JSONObject;

public class SparkAoT_1 {
	
	public static void main(String[] args) {
		/*
		 * arg0 input path
		 * arg1 output path
		 */
		
		// check argument number
		if(args.length < 2) {
			System.out.println("Require 2 arguments.");
			System.exit(1);
		}

		String input = args[0];
		String output = args[1];
		
		// essential to run any spark code
		SparkConf conf = new SparkConf().setAppName("SparkAoT_1");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// load input data to RDD
		JavaRDD<String> dataRDD = sc.textFile(input);
		
		@SuppressWarnings("serial")
		JavaPairRDD<String, Integer> result =
			    dataRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>(){
				    public Iterator<Tuple2<String, Integer>> call(String value){
				    	
						JSONObject jsonobj = new JSONObject(value.toString());
						JSONObject sub_jsonobj = jsonobj.getJSONObject("features");
						
						List<Tuple2<String, Integer>> parameter =
						     	new ArrayList<Tuple2<String, Integer>>();
						
						parameter.add(new Tuple2<String, Integer>(sub_jsonobj.getString("parameter"), 1));
						return parameter.iterator();
				    }
				}).reduceByKey(new Function2<Integer, Integer, Integer>(){
					public Integer call(Integer x, Integer y){
						return x + y;
					}
				});
		
		// save output data to disk
		result.saveAsTextFile(output);
	}
}