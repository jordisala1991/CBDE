package cbde.db;

import java.util.List;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;

public class RandomGenerator {
	
	private Random random;
	
	public RandomGenerator() {
		random = new Random();
	}
	
	private static int pow(int base, int exp) {

		int result = 1;
        while (exp != 0) {
           if ((exp & 1) == 1) result *= base;
           exp >>= 1;
           base *= base;
        }
        return result;
	}
	
	public int randomInt(int min, int max) {
		return random.nextInt(max - min)+min;
	}
	
	public int randomInt(int numDigits) {
		int min = pow(10, numDigits -1);
		int max = min*10 -1;
		return randomInt(min, max);
	}
	
	public String randomString(int numChars) {
		return RandomStringUtils.randomAlphanumeric(numChars);
	}
	
	public String randomDate() {
		int dia = randomInt(1, 28);
		int mes = randomInt(1, 12);
		int any = randomInt(1800, 2013);
		return dia + "/" + mes + "/" + any;
	}

	public Object getRandomItem(List<?> partSupp) {
		
		return partSupp.get(random.nextInt(partSupp.size()));
	}
}
