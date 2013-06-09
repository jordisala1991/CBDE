package cbde.db;

import java.net.UnknownHostException;
import java.util.ArrayList;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.BasicDBObject;

public class MongoDenormalizedScript {

	private RandomGenerator randomGenerator;
	private Mongo mongo;
	private DB db;
	
	private static final String SUPPLIER_COLLECTION = "supplier";
	private static final String CUSTOMER_COLLECTION = "customer";
	private static final String PARTSUPP_COLLECTION = "partsupp";
	private static final String LINEITEM_COLLECTION = "lineitem";
	private static final int REGION_NUM_INSERTS = 5;
	private static final int NATION_NUM_INSERTS = 25;
	private static final int SUPPLIER_NUM_INSERTS = 33;
	private static final int CUSTOMER_NUM_INSERTS = 500;
	private static final int PART_NUM_INSERTS = 666;
	private static final int ORDERS_NUM_INSERTS = 5000;
	private static final int PART_SUPP_NUM_INSERTS = 2666;
	private static final int LINE_ITEM_NUM_INSERTS = 20000;

	public MongoDenormalizedScript() throws UnknownHostException {
		
		mongo = new Mongo("localhost");
		db = mongo.getDB("cbde");
		randomGenerator = new RandomGenerator();
	}
	
	public void deleteAllCollections() {
		
		db.getCollection(SUPPLIER_COLLECTION).drop();
		db.getCollection(CUSTOMER_COLLECTION).drop();
		db.getCollection(PARTSUPP_COLLECTION).drop();
		db.getCollection(LINEITEM_COLLECTION).drop();
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
		
		DBCollection supplierCollection = db.getCollection(SUPPLIER_COLLECTION);
		
		for(int index = 1; index <= SUPPLIER_NUM_INSERTS; index++) {
			supplierCollection.save(supplier(index, randomGenerator.getRandomItem(nations)));
		}
		
	}

	private DBObject supplier(int index, Object randomNation) {
		
		BasicDBObject supplier = new BasicDBObject();
		supplier.append("s_sk", index);
		supplier.append("s_n", randomGenerator.randomString(32));
		supplier.append("s_ad", randomGenerator.randomString(32));
		supplier.append("s_nk", randomNation);
		supplier.append("s_p", randomGenerator.randomString(9));
		supplier.append("s_ac", randomGenerator.randomInt(7));
		supplier.append("s_c", randomGenerator.randomString(53));
		
		return supplier;
	}

	private ArrayList<BasicDBObject> generateNations() {
		
		ArrayList<BasicDBObject> nations = new ArrayList<BasicDBObject>();
		ArrayList<BasicDBObject> regions = generateRegions();
		
		for(int index = 1; index <= NATION_NUM_INSERTS; index++) {
			nations.add(nation(index, randomGenerator.getRandomItem(regions)));
		}
		
		return nations;
	}

	private BasicDBObject nation(int index, Object randomRegion) {

		BasicDBObject nation = new BasicDBObject();
		nation.append("n_nk", index);
		nation.append("n_n", randomGenerator.randomString(32));
		nation.append("n_rk", randomRegion);
		nation.append("n_c", randomGenerator.randomString(80));
		
		return nation;
	}

	private ArrayList<BasicDBObject> generateRegions() {
		
		ArrayList<BasicDBObject> regions = new ArrayList<BasicDBObject>();
		
		for(int index = 1; index <= REGION_NUM_INSERTS; index++) {
			regions.add(region(index));
		}
		
		return regions;
	}

	private BasicDBObject region(int index) {
		
		BasicDBObject region = new BasicDBObject();
		region.append("r_rk", index);
		region.append("r_n", randomGenerator.randomString(32));
		region.append("r_c", randomGenerator.randomString(80));
		
		return region;
	}
	
}
