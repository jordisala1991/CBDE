package cbde.db.mongo;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoHelper {
	
	private MongoHelper() {}

	public static double executeInsertMeasuringTime(DBCollection collection, DBObject document) {
		
		long startTime = System.nanoTime();
		collection.save(document);
		long endTime = System.nanoTime();
		
		return (endTime - startTime) / 1.0E09;
	}
}
