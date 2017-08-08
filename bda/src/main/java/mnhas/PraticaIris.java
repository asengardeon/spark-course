package mnhas;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.v2maestros.spark.bda.common.SparkConnection;

public class PraticaIris {
	public static void main(String[] args) {
		
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		JavaSparkContext spContext = SparkConnection.getContext();
		JavaRDD<String> autoAllData = spContext.textFile("data/iris.csv");
		System.out.println("Total Records in Iris.csv : "+ autoAllData.count());
	}

}
