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
	private static final String ORDERS_TABLE = "orders";
	private static final String PART_SUPP_TABLE = "partsupp";
	private static final String LINE_ITEM_TABLE = "lineitem";
	private static final int REGION_NUM_INSERTS = 5;
	private static final int NATION_NUM_INSERTS = 25;
	private static final int SUPPLIER_NUM_INSERTS = 33;
	private static final int CUSTOMER_NUM_INSERTS = 500;
	private static final int PART_NUM_INSERTS = 666;
	private static final int ORDERS_NUM_INSERTS = 5000;
	private static final int PART_SUPP_NUM_INSERTS = 2666;
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
		ordersInserts();
		partSuppInserts();
		//lineItemInserts();
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
	
	/////////////////////////////////////
	// A partir d'aqui es la meva part //
	/////////////////////////////////////
	private void ordersInserts() {
		
		BasicDBObject query = new BasicDBObject(TABLE, ORDERS_TABLE);
		DBObject document = findOneBy(query);
		int insertedRows = insertedRowsNumber(ORDERS_TABLE);
		int customersInserted = insertedRowsNumber(CUSTOMER_TABLE);
		
		for(int index = insertedRows + 1; index <= insertedRows + ORDERS_NUM_INSERTS; index++) {
			document.put(String.valueOf(index), orders(index, customersInserted));
		}
		
		document.put(INSERTED_ATTR, insertedRows + ORDERS_NUM_INSERTS);
		normalizedCollection.save(document);
	}
	
	private DBObject orders(int index, int customersInserted) {
		
		BasicDBObject order = new BasicDBObject();
		order.append("o_ok", index);
		order.append("o_ck", randomGenerator.randomInt(1, customersInserted));
		order.append("o_os", randomGenerator.randomString(?));
		order.append("o_tp", randomGenerator.randomInt(?));  // es decimal, ja esta b�?
		order.append("o_od", randomGenerator.randomDate());
		order.append("o_op", randomGenerator.randomString(?));
		order.append("o_cl", randomGenerator.randomString(?));
		order.append("o_sp", randomGenerator.randomInt(?));
		order.append("o_co", randomGenerator.randomString(?));
		
		return order;
	}
	
	private void partSuppInserts() {
		
		BasicDBObject query = new BasicDBObject(TABLE, PART_SUPP_TABLE);
		DBObject document = findOneBy(query);		
		int insertedRows = insertedRowsNumber(PART_SUPP_TABLE);
		int partsInserted = insertedRowsNumber(PART_TABLE);
		int suppliersInserted = insertedRowsNumber(SUPPLIER_TABLE);
		
		for(int index = insertedRows + 1; index <= insertedRows + PART_SUPP_NUM_INSERTS; index++) {
			document.put(String.valueOf(index), partSupp(partsInserted, suppliersInserted));
		}

		document.put(INSERTED_ATTR, insertedRows + PART_SUPP_NUM_INSERTS);
		normalizedCollection.save(document);
	}
	
	private DBObject partSupp(int partsInserted, int suppliersInserted) {
		
		// Aixo no esta be pq la primary key no es respecta
		BasicDBObject partSupp = new BasicDBObject();
		partSupp.append("ps_pk", randomGenerator.randomInt(1, partsInserted));
		partSupp.append("ps_sk", randomGenerator.randomInt(1, suppliersInserted));
		partSupp.append("ps_a", randomGenerator.randomInt(?));
		partSupp.append("ps_sc", randomGenerator.randomInt(?));  // es decimal, ja esta be?
		partSupp.append("ps_c", randomGenerator.randomString(?));
		
		return partSupp;
	} 
	
	private void lineItemInserts() {
		
		BasicDBObject query = new BasicDBObject(TABLE, LINE_ITEM_TABLE);
		DBObject document = findOneBy(query);
		int insertedRows = insertedRowsNumber(LINE_ITEM_TABLE);
		int ordersInserted = insertedRowsNumber(ORDERS_TABLE);
		int partsInserted = insertedRowsNumber(PART_TABLE);
		int suppliersInserted = insertedRowsNumber(SUPPLIER_TABLE);
		
		for(int index = insertedRows + 1; index <= insertedRows + LINE_ITEM_NUM_INSERTS; index++) {
			document.put(String.valueOf(index), lineItem(index, ordersInserted, partsInserted, suppliersInserted));	
		}
		
		document.put(INSERTED_ATTR, insertedRows + LINE_ITEM_NUM_INSERTS);
		normalizedCollection.save(document);
	}

	private DBObject lineItem(int index, int ordersInserted, int partsInserted, int suppliersInserted) {
		
		// Aixo no tinc clar que sigui correcte
		BasicDBObject lineItem = new BasicDBObject();
		lineItem.append("l_ok", randomGenerator.randomInt(1, ordersInserted));
		lineItem.append("l_pk", randomGenerator.randomInt(1, partsInserted));
		lineItem.append("l_sk",randomGenerator.randomInt(1, suppliersInserted) );
		lineItem.append("l_ln", index);
		lineItem.append("l_q", randomGenerator.randomInt(?));
		lineItem.append("l_ep", randomGenerator.randomInt(?));  // es decimal, ja esta be?
		lineItem.append("l_d", randomGenerator.randomInt(?));  // es decimal, ja esta be?
		lineItem.append("l_t", randomGenerator.randomInt(?));  // es decimal, ja esta be?
		lineItem.append("l_rf", randomGenerator.randomString(?));
		lineItem.append("l_ls", randomGenerator.randomString(?));
		lineItem.append("l_sd", randomGenerator.randomDate());
		lineItem.append("l_cd", randomGenerator.randomDate());
		lineItem.append("l_rd", randomGenerator.randomDate());
		lineItem.append("l_ss", randomGenerator.randomString(?));
		lineItem.append("l_sm", randomGenerator.randomString(?));
		lineItem.append("l_c", randomGenerator.randomString(?));
		return lineItem;
	}
	
}
