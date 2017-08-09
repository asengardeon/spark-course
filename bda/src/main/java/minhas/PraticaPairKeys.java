package minhas;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.v2maestros.spark.bda.common.SparkConnection;

import scala.Tuple2;

public class PraticaPairKeys {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		JavaSparkContext spContext = SparkConnection.getContext();
		JavaRDD<String> autoAllData = spContext.textFile("data/iris.csv");
		
		//remove header and filter no value
		String header = autoAllData.first();
		JavaRDD<String> filtered = autoAllData.filter(str -> !str.equals(header));
				
		JavaPairRDD<String, Double[]> species = filtered.mapToPair(new internalPairFunction());
		
		System.out.println("KV RDD Demo - raw tuples :");
		for (Tuple2<String, Double[]> kvList : species.take(10)) {
			System.out.println(kvList._1 + " - "+ kvList._2[0]);
		}
		
		
		JavaPairRDD<String, Double[]> sumarized = species.reduceByKey(new computeMinLenght());
		
		System.out.println("KV RDD Demo - Show the minimum SepalLength for each specie :");
		for (Tuple2<String, Double[]> kvList : sumarized.take(5)) {
			System.out.println(kvList._1 + " - " + kvList._2[0] );
		}

	}

}
