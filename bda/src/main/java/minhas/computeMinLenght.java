package minhas;

import org.apache.spark.api.java.function.Function2;

public class computeMinLenght implements Function2<Double[], Double[], Double[]> {

	@Override
	public Double[] call(Double[] v1, Double[] v2) throws Exception {		
		Double[] result;
		if(v1[0] <= v2[0]) {
			result = new Double[]{v1[0]};
		}else {
			result = new Double[]{v2[0]};
		}
		return result;
	}

}
