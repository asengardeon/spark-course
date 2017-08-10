package sparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Column;

import com.v2maestros.spark.bda.common.SparkConnection;

import static org.apache.spark.sql.functions.*;

public class Dataframes {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkSession spSession = SparkConnection.getSession();
		JavaSparkContext spContext = SparkConnection.getContext();
		
		Dataset<Row> irisDf = spSession.read().csv("data/iris.csv");
		irisDf.show();
				
		Dataset<Row> filtered = irisDf.filter(col("_c3").$greater("0.4"));
		
		filtered.show();
		
		System.out.println("Counted: "+filtered.count());
		
		
		

	}

}
