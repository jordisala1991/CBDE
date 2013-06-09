package cbde.db;

import java.net.UnknownHostException;
import java.util.ArrayList;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.BasicDBObject;

public class MongoDenormalizedScript {

	private RandomGenerator randomGenerator;
	private Mongo mongo;
	private DB db;

	public MongoDenormalizedScript() throws UnknownHostException {
		
		mongo = new Mongo("localhost");
		db = mongo.getDB("cbde");
		randomGenerator = new RandomGenerator();
	}

	public void randomInserts() {
		ArrayList<BasicDBObject> nations = generateNations();
		supplierInserts(nations);
		customerInserts(nations);
		partSuppInserts();
		lineItemInserts();
	}

	private void lineItemInserts() {
		// TODO Auto-generated method stub
		
	}

	private void partSuppInserts() {
		// TODO Auto-generated method stub
		
	}

	private void customerInserts(ArrayList<BasicDBObject> nations) {
		// TODO Auto-generated method stub
		
	}

	private void supplierInserts(ArrayList<BasicDBObject> nations) {
		// TODO Auto-generated method stub
		
	}

	private ArrayList<BasicDBObject> generateNations() {
		ArrayList<BasicDBObject> nations = new ArrayList<BasicDBObject>();
		
		return nations;
	}
	
}
