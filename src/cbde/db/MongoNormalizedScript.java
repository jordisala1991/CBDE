package cbde.db;

import java.net.UnknownHostException;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoNormalizedScript {
	
	private Mongo mongo;
	private DB db;
	private DBCollection normalizedCollection;
	private RandomGenerator randomGenerator;
	
	private static final String NORMALIZED_COLLECTION = "norm_collection";
	private static final String TABLE = "table";
	private static final String INSERTED_ATTR = "inserted";
	private static final String REGION_TABLE = "region";
	private static final String NATION_TABLE = "nation";
	private static final String SUPPLIER_TABLE = "supplier";
	private static final String CUSTOMER_TABLE = "customer";
	private static final String PART_TABLE = "part";
	private static final String LINE_ITEM_TABLE = "lineitem";
	private static final int REGION_NUM_INSERTS = 5;
	private static final int NATION_NUM_INSERTS = 25;
	private static final int SUPPLIER_NUM_INSERTS = 33;
	private static final int CUSTOMER_NUM_INSERTS = 500;
	private static final int PART_NUM_INSERTS = 666;
	private static final int LINE_ITEM_NUM_INSERTS = 20000;

	public MongoNormalizedScript() throws UnknownHostException {
		
		mongo = new Mongo("localhost");
		db = mongo.getDB("cbde");
		randomGenerator = new RandomGenerator();
		normalizedCollection = db.getCollectionFromString(NORMALIZED_COLLECTION);
	}
	
	public void deleteCollection() {
		
		normalizedCollection.drop();
	}
	
	public void randomInserts() {
		
		regionInserts();
		nationInserts();
		supplierInserts();
		customerInserts();
		partInserts();
		lineItemInserts();
	}

	private DBObject findOneBy(BasicDBObject query) {
		
		DBObject document = normalizedCollection.findOne(query);
		
		if (document == null) document = query;
		return document;
	}
	
	private int insertedRowsNumber(String tableName) {
		
		BasicDBObject query = new BasicDBObject(TABLE, tableName);
		BasicDBObject params = new BasicDBObject(INSERTED_ATTR, true);
		DBObject document = normalizedCollection.findOne(query, params);
		
		if (document != null) {
			return (Integer) document.get(INSERTED_ATTR);
		}
		return 0;
	}
	
	private void partInserts() {
		
		BasicDBObject query = new BasicDBObject(TABLE, PART_TABLE);
		DBObject document = findOneBy(query);
		int insertedRows = insertedRowsNumber(PART_TABLE);
		
		for(int index = insertedRows + 1; index <= insertedRows + PART_NUM_INSERTS; index++) {
			document.put(String.valueOf(index), part(index));
		}
		
		document.put(INSERTED_ATTR, insertedRows + PART_NUM_INSERTS);
		normalizedCollection.save(document);
	}
	
	private DBObject part(int index) {
		
		BasicDBObject part = new BasicDBObject();
		part.append("p_pk", index);
		part.append("p_n", randomGenerator.randomString(32));
		part.append("p_mf", randomGenerator.randomString(32));
		part.append("p_b", randomGenerator.randomString(32));
		part.append("p_t", randomGenerator.randomString(32));
		part.append("p_s", randomGenerator.randomInt(4));
		part.append("p_con", randomGenerator.randomString(32));
		part.append("p_r", randomGenerator.randomInt(7));
		part.append("p_com", randomGenerator.randomString(32));
		
		return part;
	}

	private void customerInserts() {
	
		BasicDBObject query = new BasicDBObject(TABLE, CUSTOMER_TABLE);
		DBObject document = findOneBy(query);
		int insertedRows = insertedRowsNumber(CUSTOMER_TABLE);
		int nationsInserted = insertedRowsNumber(NATION_TABLE);
		
		for(int index = insertedRows + 1; index <= insertedRows + CUSTOMER_NUM_INSERTS; index++) {
			document.put(String.valueOf(index), customer(index, nationsInserted));
		}
		
		document.put(INSERTED_ATTR, insertedRows + CUSTOMER_NUM_INSERTS);
		normalizedCollection.save(document);
	}
	
	private DBObject customer(int index, int nationsInserted) {
		
		BasicDBObject customer = new BasicDBObject();
		customer.append("c_ck", index);
		customer.append("c_n", randomGenerator.randomString(32));
		customer.append("c_ad", randomGenerator.randomString(32));
		customer.append("c_nk", randomGenerator.randomInt(1, nationsInserted));
		customer.append("c_p", randomGenerator.randomString(32));
		customer.append("c_ac", randomGenerator.randomInt(7));
		customer.append("c_m", randomGenerator.randomString(32));
		customer.append("c_c", randomGenerator.randomString(60));
		
		return customer;
	}

	private void supplierInserts() {
		
		BasicDBObject query = new BasicDBObject(TABLE, SUPPLIER_TABLE);
		DBObject document = findOneBy(query);
		int insertedRows = insertedRowsNumber(SUPPLIER_TABLE);
		int nationsInserted = insertedRowsNumber(NATION_TABLE);
		
		for(int index = insertedRows + 1; index <= insertedRows + SUPPLIER_NUM_INSERTS; index++) {
			document.put(String.valueOf(index), supplier(index, nationsInserted));
		}
		
		document.put(INSERTED_ATTR, insertedRows + SUPPLIER_NUM_INSERTS);
		normalizedCollection.save(document);
	}

	
	private DBObject supplier(int index, int nationsInserted) {
		
		BasicDBObject supplier = new BasicDBObject();
		supplier.append("s_sk", index);
		supplier.append("s_n", randomGenerator.randomString(32));
		supplier.append("s_ad", randomGenerator.randomString(32));
		supplier.append("s_nk", randomGenerator.randomInt(1, nationsInserted));
		supplier.append("s_p", randomGenerator.randomString(9));
		supplier.append("s_ac", randomGenerator.randomInt(7));
		supplier.append("s_c", randomGenerator.randomString(53));
		
		return supplier;
	}

	private void nationInserts() {
		
		BasicDBObject query = new BasicDBObject(TABLE, NATION_TABLE);
		DBObject document = findOneBy(query);
		
		int regionsInserted = insertedRowsNumber(REGION_TABLE);
		
		for(int index = 1; index <= NATION_NUM_INSERTS; index++) {
			document.put(String.valueOf(index), nation(index, regionsInserted));
		}
		
		document.put(INSERTED_ATTR, NATION_NUM_INSERTS);
		normalizedCollection.save(document);
	}
	
	private DBObject nation(int index, int regionsInserted) {
		
		BasicDBObject nation = new BasicDBObject();
		nation.append("n_nk", index);
		nation.append("n_n", randomGenerator.randomString(32));
		nation.append("n_rk", randomGenerator.randomInt(1, regionsInserted));
		nation.append("n_c", randomGenerator.randomString(80));
		
		return nation;
	}

	private void regionInserts() {
		
		BasicDBObject query = new BasicDBObject(TABLE, REGION_TABLE);
		DBObject document = findOneBy(query);
		
		for(int index = 1; index <= REGION_NUM_INSERTS; index++) {
			document.put(String.valueOf(index), region(index));
		}
		
		document.put(INSERTED_ATTR, REGION_NUM_INSERTS);
		normalizedCollection.save(document);
	}
	
	private DBObject region(int index) {
		
		BasicDBObject region = new BasicDBObject();
		region.append("r_rk", index);
		region.append("r_n", randomGenerator.randomString(32));
		region.append("r_c", randomGenerator.randomString(80));
		
		return region;
	}
	
	private void lineItemInserts() {
		
		BasicDBObject query = new BasicDBObject(TABLE, LINE_ITEM_TABLE);
		DBObject document = findOneBy(query);
		int insertedRows = insertedRowsNumber(LINE_ITEM_TABLE);
		
		for(int i = insertedRows + 1; i <= insertedRows + LINE_ITEM_NUM_INSERTS; i++) {
			document.put(String.valueOf(i), lineItem());	
		}
		
		document.put(INSERTED_ATTR, insertedRows + LINE_ITEM_NUM_INSERTS);
		normalizedCollection.save(document);
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
