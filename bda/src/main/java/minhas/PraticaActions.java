package minhas;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.v2maestros.spark.bda.common.SparkConnection;

public class PraticaActions {
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		JavaSparkContext spContext = SparkConnection.getContext();
		JavaRDD<String> autoAllData = spContext.textFile("data/iris.csv");
		
		//remove header and filter no value
		String header = autoAllData.first();
		JavaRDD<String> filtered = autoAllData.filter(str -> !str.equals(header));
				
		String average = filtered.reduce(new ActionFunction());
		double value = Double.parseDouble(average) / filtered.count();
		System.out.println("Result = "+ String.valueOf(value));
	}

}
