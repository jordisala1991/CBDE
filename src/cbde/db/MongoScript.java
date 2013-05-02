package cbde.db;

import java.net.UnknownHostException;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoScript {
	
	private Mongo mongo;
	private DB db;
	private DBCollection normalizedCollection;
	private RandomGenerator randomGenerator;
	
	private static final String NORMALIZED_COLLECTION = "norm_collection";
	private static final String TABLE = "table";
	private static final int LINE_ITEM_NUM_INSERTS = 20000;
	private static final int REGION_NUM_INSERTS = 5;
	private static final int NATION_NUM_INSERTS = 25;

	public MongoScript() throws UnknownHostException {
		
		mongo = new Mongo("localhost");
		db = mongo.getDB("cbde");
		randomGenerator = new RandomGenerator();
	}
	
	public void deleteNormalizedCollection() {
		
		normalizedCollection.drop();
	}
	
	public void randomNormalizedInserts() {
		
		normalizedCollection = db.getCollectionFromString(NORMALIZED_COLLECTION);
		regionInserts();
		nationInserts();
		//lineItemInserts();
	}
	
	private DBObject findOneBy(BasicDBObject query) {
		
		DBObject document = normalizedCollection.findOne(query);
		
		if (document == null) document = query;
		return document;
	}
	
	private int insertedRowsNumber(String tableName) {
		
		BasicDBObject query = new BasicDBObject(TABLE, tableName);
		BasicDBObject params = new BasicDBObject("inserted", true);
		DBObject document = normalizedCollection.findOne(query, params);
		
		if (document != null) {
			return (Integer) document.get("inserted");
		}
		return 0;
	}
	
	private void nationInserts() {
		
		BasicDBObject query = new BasicDBObject(TABLE, "nation");
		DBObject document = findOneBy(query);
		
		int regionsInserted = insertedRowsNumber("region");
		
		for(int index = 1; index <= NATION_NUM_INSERTS; index++) {
			document.put(String.valueOf(index), nationItem(index, regionsInserted));
		}
		normalizedCollection.save(document);
	}
	
	private DBObject nationItem(int index, int regionsInserted) {
		
		BasicDBObject nationItem = new BasicDBObject();
		nationItem.append("n_nk", index);
		nationItem.append("n_n", randomGenerator.randomString(32));
		nationItem.append("n_rk", randomGenerator.randomInt(1, regionsInserted));
		nationItem.append("n_c", randomGenerator.randomString(80));
		
		return nationItem;
	}

	private void regionInserts() {
		
		BasicDBObject query = new BasicDBObject(TABLE, "region");
		DBObject document = findOneBy(query);
		
		for(int index = 1; index <= REGION_NUM_INSERTS; index++) {
			document.put(String.valueOf(index), regionItem(index));
		}
		normalizedCollection.save(document);
	}
	
	private DBObject regionItem(int index) {
		
		BasicDBObject lineItem = new BasicDBObject();
		lineItem.append("r_rk", index);
		lineItem.append("r_n", randomGenerator.randomString(32));
		lineItem.append("r_c", randomGenerator.randomString(80));
		
		return lineItem;
	}
	
	private void lineItemInserts() {
		
		BasicDBObject query = new BasicDBObject(TABLE, "lineitem");
		DBObject document = findOneBy(query);
		int insertedRows = insertedRowsNumber("lineitem");
		
		for(int i = insertedRows + 1; i <= insertedRows + LINE_ITEM_NUM_INSERTS; i++) {
			document.put(String.valueOf(i), lineItem());	
		}
		
		document.put("inserted", insertedRows + LINE_ITEM_NUM_INSERTS);
		
		//collection.save(document);
	}

	private DBObject lineItem() {
		
		BasicDBObject lineItem = new BasicDBObject();
		lineItem.append("orderkey", 2);
		lineItem.append("orderkey", "21");
		lineItem.append("orderkey", "21");
		lineItem.append("orderkey", "21");
		lineItem.append("orderkey", "21");
		lineItem.append("orderkey", "21");
		lineItem.append("orderkey", "21");
		lineItem.append("orderkey", "21");
		lineItem.append("orderkey", "21");
		lineItem.append("orderkey", "21");
		lineItem.append("orderkey", "21");
		lineItem.append("orderkey", "21");
		lineItem.append("orderkey", "21");
		lineItem.append("orderkey", "21");
		lineItem.append("orderkey", "21");
		lineItem.append("orderkey", "21");
		return lineItem;
	}
	
}
