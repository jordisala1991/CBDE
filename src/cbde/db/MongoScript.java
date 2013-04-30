package cbde.db;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.Mongo;

public class MongoScript {
	
	private Mongo mongo;
	private DB db;

	public MongoScript() throws UnknownHostException {
		mongo = new Mongo("localhost");
		db = mongo.getDB("cbde");
	}
	
}
