package minhas;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class internalPairFunction implements PairFunction<String, String, Double[]> {

	@Override
	public Tuple2<String, Double[]> call(String line) throws Exception {
		String[] values = line.split(",");
		Double[] sepalLenght = {Double.parseDouble(values[0])};
		return new Tuple2<String, Double[]>(values[4], sepalLenght);
		
		
	}	

}
