package minhas;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.v2maestros.spark.bda.common.ExerciseUtils;
import com.v2maestros.spark.bda.common.SparkConnection;

public class PraticaTransformacao {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		JavaSparkContext spContext = SparkConnection.getContext();
		JavaRDD<String> autoAllData = spContext.textFile("data/iris.csv");
		
		//remove header and filter no value
		String header = autoAllData.first();
		JavaRDD<String> filtered = autoAllData.filter(str -> str.contains("versicolor"));
				
		JavaRDD<String> transformed = filtered.map(new InternalFunction());
		int qtd = (int) transformed.count();
		ExerciseUtils.printStringRDD(transformed, qtd);
		
	}
	
	
}



