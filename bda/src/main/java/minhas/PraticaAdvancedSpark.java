package minhas;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import com.v2maestros.spark.bda.common.ExerciseUtils;
import com.v2maestros.spark.bda.common.SparkConnection;

public class PraticaAdvancedSpark {
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
		Broadcast<Double> averageSepalLenght = spContext.broadcast(value);
		LongAccumulator averageGreaterCount = spContext.sc().longAccumulator();
		LongAccumulator averageLowerCount = spContext.sc().longAccumulator();
		JavaRDD<String> valuesGreaterThanAverage = filtered.filter(new Function<String, Boolean>() {			
			@Override
			public Boolean call(String v1) throws Exception {
				String[] splited = v1.split(",");
				boolean result =  Double.parseDouble(splited[0]) > averageSepalLenght.value();
				if(result) {
					averageGreaterCount.add(1);
				}else {
					averageLowerCount.add(1);
				}
				return result;
			}
		});
		
		JavaRDD<String> valuesLowerThanAverage = filtered.filter(new Function<String, Boolean>() {			
			@Override
			public Boolean call(String v1) throws Exception {
				String[] splited = v1.split(",");
				return Double.parseDouble(splited[0]) <= averageSepalLenght.value();
			}
		});
		
		System.out.println("A média é: "+value);
		System.out.println("Os que são acima da média são: "); 
		ExerciseUtils.printStringRDD(valuesGreaterThanAverage, 100);
		System.out.println("Quantidade: "+averageGreaterCount.value());
		System.out.println("Os que são abaixo ou igual a média são:");
		ExerciseUtils.printStringRDD(valuesLowerThanAverage, 100);
		System.out.println("Quantidade: "+averageLowerCount.value());
		ExerciseUtils.hold();
	}
}

