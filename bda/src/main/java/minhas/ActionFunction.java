package minhas;

import org.apache.spark.api.java.function.Function2;

public class ActionFunction implements Function2<String, String, String> {

	@Override
	public String call(String v1, String v2) throws Exception {
		double value1, value2;
		value1 = getSepalLenght(v1);
		value2 = getSepalLenght(v2);
		return String.valueOf((value1+value2));
	}
	
	private double getSepalLenght(String line) {
		return Double.parseDouble(line.split(",")[0]);		
	}

}
