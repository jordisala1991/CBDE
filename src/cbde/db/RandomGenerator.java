package cbde.db;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;

public class RandomGenerator {
	
	private Random random;
	private GregorianCalendar calendar;
	
	public RandomGenerator() {
		random = new Random();
		calendar = new GregorianCalendar(0, 0, 0);
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
		return random.nextInt(max - min + 1)+min;
	}
	
	public int randomInt(int numDigits) {
		int min = pow(10, numDigits - 1);
		int max = min*10 - 1;
		return randomInt(min, max);
	}
	
	public String randomString(int numChars) {
		return RandomStringUtils.randomAlphanumeric(numChars);
	}
	
	public Date randomDate() {

		calendar.set(Calendar.YEAR, randomInt(1901, 2013));
		calendar.set(Calendar.MONTH, randomInt(0, 11));
		int maxDay = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
		calendar.set(Calendar.DAY_OF_MONTH, randomInt(1, maxDay));
		
		return calendar.getTime();
	}

	public Object getRandomItem(List<?> list) {
		
		return list.get(random.nextInt(list.size()));
	}
}
