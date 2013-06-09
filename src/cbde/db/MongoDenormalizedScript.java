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

		DBCollection partSuppCollection = db.getCollection(PARTSUPP_COLLECTION);
		ArrayList<BasicDBObject> parts = generateParts();
		
		for(int index = 1; index <= PART_SUPP_NUM_INSERTS; index++) {
			partSuppCollection.save(partSupp(index, randomGenerator.getRandomItem(parts)));
		}	
	}

	private DBObject partSupp(int index, Object randomPart) {
		
		BasicDBObject partSupp = new BasicDBObject();
		partSupp.append("ps_id", index);
		partSupp.append("ps_pk", randomPart);
		partSupp.append("ps_sk", randomGenerator.randomInt(1, SUPPLIER_NUM_INSERTS));
		partSupp.append("ps_a", randomGenerator.randomInt(4));
		partSupp.append("ps_sc", randomGenerator.randomInt(7));
		partSupp.append("ps_c", randomGenerator.randomString(100));
		
		return partSupp;
	}

	private ArrayList<BasicDBObject> generateParts() {
		ArrayList<BasicDBObject> parts = new ArrayList<BasicDBObject>();
		
		for(int index = 1; index <= PART_NUM_INSERTS; index++) {
			parts.add(part(index));
		}
		
		return parts;
	}

	private BasicDBObject part(int index) {
		
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

	private void customerInserts(ArrayList<BasicDBObject> nations) {

		DBCollection customerCollection = db.getCollection(CUSTOMER_COLLECTION);
		
		for(int index = 1; index <= CUSTOMER_NUM_INSERTS; index++) {
			customerCollection.save(customer(index, randomGenerator.getRandomItem(nations)));
		}
		
	}
	
	private DBObject customer(int index, Object randomNation) {
		
		BasicDBObject customer = new BasicDBObject();
		customer.append("c_ck", index);
		customer.append("c_n", randomGenerator.randomString(32));
		customer.append("c_ad", randomGenerator.randomString(32));
		customer.append("c_nk", randomNation);
		customer.append("c_p", randomGenerator.randomString(32));
		customer.append("c_ac", randomGenerator.randomInt(7));
		customer.append("c_m", randomGenerator.randomString(32));
		customer.append("c_c", randomGenerator.randomString(60));
		
		return customer;

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
