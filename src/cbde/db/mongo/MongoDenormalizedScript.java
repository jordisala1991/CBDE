package cbde.db.mongo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import cbde.db.RandomGenerator;
import cbde.db.mongo.MongoHelper;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.BasicDBObject;

public class MongoDenormalizedScript {

	private RandomGenerator randomGenerator;
	private Mongo mongo;
	private DB db;
	private double insertsTime;
	private int insertedTimes;
	private ArrayList<BasicDBObject> nations;
	
	private static final String SUPPLIER_COLLECTION = "supplier";
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
		db.getCollection(PARTSUPP_COLLECTION).drop();
		db.getCollection(LINEITEM_COLLECTION).drop();
	}

	public void randomInserts(int insertedTimes) {
		
		this.insertedTimes = insertedTimes;
		insertsTime = 0;
		if (insertedTimes == 0) {
			nations = generateNations();	
		}
		supplierInserts(nations);
		ArrayList<BasicDBObject> customers = generateCustomers(nations);
		partSuppInserts();
		lineItemInserts(customers);
		System.out.println("Inserts time: " + insertsTime + " seconds");
		System.out.println("------------------------------------");
	}

	private void lineItemInserts(ArrayList<BasicDBObject> customers) {

		DBCollection lineItemCollection = db.getCollection(LINEITEM_COLLECTION);
		
		ArrayList<BasicDBObject> orders = generateOrders(customers);
		
		for(int index = 1; index <= LINE_ITEM_NUM_INSERTS; index++) {
			DBObject lineItem = lineItem(index + insertedTimes*LINE_ITEM_NUM_INSERTS, randomGenerator.getRandomItem(orders));
			insertsTime += MongoHelper.executeInsertMeasuringTime(lineItemCollection, lineItem);
		}
		
	}

	private DBObject lineItem(int index, Object randomOrder) {
		
		BasicDBObject lineItem = new BasicDBObject();
		lineItem.append("l_ok", randomOrder);
		lineItem.append("l_pk", randomGenerator.randomInt(1, PART_NUM_INSERTS));
		lineItem.append("l_sk", randomGenerator.randomInt(1, SUPPLIER_NUM_INSERTS));
		lineItem.append("l_ln", index);
		lineItem.append("l_q", randomGenerator.randomInt(4));
		lineItem.append("l_ep", randomGenerator.randomInt(7));
		lineItem.append("l_d", randomGenerator.randomInt(7));
		lineItem.append("l_t", randomGenerator.randomInt(7));
		lineItem.append("l_rf", randomGenerator.randomString(32));
		lineItem.append("l_ls", randomGenerator.randomString(32));
		lineItem.append("l_sd", randomGenerator.randomDate());
		lineItem.append("l_cd", randomGenerator.randomDate());
		lineItem.append("l_rd", randomGenerator.randomDate());
		lineItem.append("l_ss", randomGenerator.randomString(32));
		lineItem.append("l_sm", randomGenerator.randomString(32));
		lineItem.append("l_c", randomGenerator.randomString(32));
		return lineItem;
		
	}

	private ArrayList<BasicDBObject> generateOrders(ArrayList<BasicDBObject> customers) {
	
		ArrayList<BasicDBObject> orders = new ArrayList<BasicDBObject>();
		
		for(int index = 1; index <= ORDERS_NUM_INSERTS; index++) {
			orders.add(order(index + insertedTimes*ORDERS_NUM_INSERTS, randomGenerator.getRandomItem(customers)));
		}
		
		return orders;
		
	}

	private BasicDBObject order(int index, Object randomOrder) {
		
		BasicDBObject order = new BasicDBObject();
		order.append("o_ok", index);
		order.append("o_ck", randomOrder);
		order.append("o_os", randomGenerator.randomString(32));
		order.append("o_tp", randomGenerator.randomInt(7));
		order.append("o_od", randomGenerator.randomDate());
		order.append("o_op", randomGenerator.randomString(8));
		order.append("o_cl", randomGenerator.randomString(32));
		order.append("o_sp", randomGenerator.randomInt(4));
		order.append("o_co", randomGenerator.randomString(40));
		
		return order;
	}
	
    private void partSuppInserts() {

        DBCollection partSuppCollection = db.getCollection(PARTSUPP_COLLECTION);
        ArrayList<BasicDBObject> parts = generateParts();
        
        for(int index = 1; index <= PART_SUPP_NUM_INSERTS; index++) {
        	DBObject partSupp = partSupp(index + insertedTimes*PART_NUM_INSERTS, randomGenerator.getRandomItem(parts));
        	MongoHelper.executeInsertMeasuringTime(partSuppCollection, partSupp);
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
            parts.add(part(index + insertedTimes*PART_NUM_INSERTS));
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

	private ArrayList<BasicDBObject> generateCustomers(ArrayList<BasicDBObject> nations) {

		ArrayList<BasicDBObject> customers = new ArrayList<BasicDBObject>();
		
		for(int index = 1; index <= CUSTOMER_NUM_INSERTS; index++) {
			customers.add(customer(index + insertedTimes*CUSTOMER_NUM_INSERTS, randomGenerator.getRandomItem(nations)));
		}
		
		return customers;
		
	}
	
	private BasicDBObject customer(int index, Object randomNation) {
		
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
			DBObject supplier = supplier(index + insertedTimes*SUPPLIER_NUM_INSERTS, randomGenerator.getRandomItem(nations));
			MongoHelper.executeInsertMeasuringTime(supplierCollection, supplier);
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
			nations.add(nation(index + insertedTimes*NATION_NUM_INSERTS, randomGenerator.getRandomItem(regions)));
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
			regions.add(region(index + insertedTimes*REGION_NUM_INSERTS));
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
