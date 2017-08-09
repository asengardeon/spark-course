package minhas;

import org.apache.spark.api.java.function.Function;

public class InternalFunction implements Function<String, String> {

	@Override
	public String call(String s) throws Exception {
		String[] attLIst = s.split(",");
		String result = "";
		for (int i = 0; i < attLIst.length - 1; i++) {
			double vl = Double.parseDouble(attLIst[i]);
			result += String.valueOf(vl) + ",";
		}
		result += attLIst[attLIst.length-1];
		return result;
	}

}
