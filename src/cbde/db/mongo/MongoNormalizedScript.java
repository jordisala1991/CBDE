package cbde.db.mongo;

import java.net.UnknownHostException;

import cbde.db.RandomGenerator;

import com.mongodb.BasicDBList;
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
	private double insertsTime = 0;
	
	private static final String NORMALIZED_COLLECTION = "norm_collection";
	private static final String TABLE = "table";
	private static final String INSERTED_ATTR = "inserted";
	private static final String ROWS_ATTR = "rows";
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
		
		insertsTime = 0;
		regionInserts();
		nationInserts();
		supplierInserts();
		customerInserts();
		partInserts();
		ordersInserts();
		partSuppInserts();
		lineItemInserts();
		System.out.println("Inserts time: " + insertsTime + " seconds");
		System.out.println("------------------------------------");
	}

	private DBObject findOneBy(BasicDBObject query) {
		
		DBObject document = normalizedCollection.findOne(query);
		
		if (document == null) {
			document = query;
			document.put(INSERTED_ATTR, 0);
			document.put(ROWS_ATTR, new BasicDBList());
		}
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
	
	private BasicDBList getListOfProjectedRows(String tableName, DBObject projection) {
		
		BasicDBObject query = new BasicDBObject(TABLE, tableName);
		DBObject result = normalizedCollection.findOne(query, projection);
		
		if (result == null) {
			return new BasicDBList();
		}
		return (BasicDBList) result.get("rows");
	}
	
	private void partInserts() {
		
		BasicDBObject query = new BasicDBObject(TABLE, PART_TABLE);
		DBObject document = findOneBy(query);
		BasicDBList rows = (BasicDBList) document.get(ROWS_ATTR);
		int insertedRows = insertedRowsNumber(PART_TABLE);

		document.put(INSERTED_ATTR, insertedRows + PART_NUM_INSERTS);
		for(int index = 1; index <= PART_NUM_INSERTS; index++) {
			rows.add(part(insertedRows + index));
		}
		
		insertsTime += MongoHelper.executeInsertMeasuringTime(normalizedCollection, document);
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
		BasicDBList rows = (BasicDBList) document.get(ROWS_ATTR);
		int insertedRows = insertedRowsNumber(CUSTOMER_TABLE);
		int nationsInserted = insertedRowsNumber(NATION_TABLE);

		document.put(INSERTED_ATTR, insertedRows + CUSTOMER_NUM_INSERTS);
		for(int index = 1; index <= CUSTOMER_NUM_INSERTS; index++) {
			rows.add(customer(insertedRows + index, nationsInserted));
		}
		
		insertsTime += MongoHelper.executeInsertMeasuringTime(normalizedCollection, document);
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
		BasicDBList rows = (BasicDBList) document.get(ROWS_ATTR);
		int insertedRows = insertedRowsNumber(SUPPLIER_TABLE);
		int nationsInserted = insertedRowsNumber(NATION_TABLE);

		document.put(INSERTED_ATTR, insertedRows + SUPPLIER_NUM_INSERTS);
		for(int index = 1; index <= SUPPLIER_NUM_INSERTS; index++) {
			rows.add(supplier(insertedRows + index, nationsInserted));
		}
		
		insertsTime += MongoHelper.executeInsertMeasuringTime(normalizedCollection, document);
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
		BasicDBList rows = (BasicDBList) document.get(ROWS_ATTR);
		rows.clear();
		int regionsInserted = insertedRowsNumber(REGION_TABLE);

		document.put(INSERTED_ATTR, NATION_NUM_INSERTS);
		for(int index = 1; index <= NATION_NUM_INSERTS; index++) {
			rows.add(nation(index, regionsInserted));
		}
		
		insertsTime += MongoHelper.executeInsertMeasuringTime(normalizedCollection, document);
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
		BasicDBList rows = (BasicDBList) document.get(ROWS_ATTR);
		rows.clear();

		document.put(INSERTED_ATTR, REGION_NUM_INSERTS);
		for(int index = 1; index <= REGION_NUM_INSERTS; index++) {
			rows.add(region(index));
		}
		
		insertsTime += MongoHelper.executeInsertMeasuringTime(normalizedCollection, document);
	}
	
	private DBObject region(int index) {
		
		BasicDBObject region = new BasicDBObject();
		region.append("r_rk", index);
		region.append("r_n", randomGenerator.randomString(32));
		region.append("r_c", randomGenerator.randomString(80));
		
		return region;
	}
	
	private void ordersInserts() {
		
		BasicDBObject query = new BasicDBObject(TABLE, ORDERS_TABLE);
		DBObject document = findOneBy(query);
		BasicDBList rows = (BasicDBList) document.get(ROWS_ATTR);
		int insertedRows = insertedRowsNumber(ORDERS_TABLE);
		int customersInserted = insertedRowsNumber(CUSTOMER_TABLE);

		document.put(INSERTED_ATTR, insertedRows + ORDERS_NUM_INSERTS);
		for(int index = 1; index <= ORDERS_NUM_INSERTS; index++) {
			rows.add(orders(insertedRows + index, customersInserted));
		}
		
		insertsTime += MongoHelper.executeInsertMeasuringTime(normalizedCollection, document);
	}
	
	private DBObject orders(int index, int customersInserted) {
		
		BasicDBObject order = new BasicDBObject();
		order.append("o_ok", index);
		order.append("o_ck", randomGenerator.randomInt(1, customersInserted));
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
		
		BasicDBObject query = new BasicDBObject(TABLE, PART_SUPP_TABLE);
		DBObject document = findOneBy(query);
		BasicDBList rows = (BasicDBList) document.get(ROWS_ATTR);
		int insertedRows = insertedRowsNumber(PART_SUPP_TABLE);
		int partsInserted = insertedRowsNumber(PART_TABLE);
		int suppliersInserted = insertedRowsNumber(SUPPLIER_TABLE);
		
		BasicDBObject pk_partsup = new BasicDBObject();
		pk_partsup.append("rows.ps_pk", true);
		pk_partsup.append("rows.ps_sk", true);
		BasicDBList partSuppRows = getListOfProjectedRows(PART_SUPP_TABLE, pk_partsup);
		
		document.put(INSERTED_ATTR, insertedRows + PART_SUPP_NUM_INSERTS);		
		for(int index = 1; index <= PART_SUPP_NUM_INSERTS; index++) {
			BasicDBObject randomPK = new BasicDBObject();
			do {
				randomPK.append("ps_pk", randomGenerator.randomInt(1, partsInserted));
				randomPK.append("ps_sk", randomGenerator.randomInt(1, suppliersInserted));
			} while(partSuppRows.contains(randomPK));
			rows.add(partSupp(randomPK));
		}

		insertsTime += MongoHelper.executeInsertMeasuringTime(normalizedCollection, document);
	}
	
	private DBObject partSupp(BasicDBObject primaryKey) {
		
		BasicDBObject partSupp = new BasicDBObject();
		partSupp.append("ps_pk", primaryKey.get("ps_pk"));
		partSupp.append("ps_sk", primaryKey.get("ps_sk"));
		partSupp.append("ps_a", randomGenerator.randomInt(4));
		partSupp.append("ps_sc", randomGenerator.randomInt(7));
		partSupp.append("ps_c", randomGenerator.randomString(100));
		
		return partSupp;
	} 
	
	private void lineItemInserts() {
		
		BasicDBObject query = new BasicDBObject(TABLE, LINE_ITEM_TABLE);
		DBObject document = findOneBy(query);
		BasicDBList rows = (BasicDBList) document.get(ROWS_ATTR);
		int insertedRows = insertedRowsNumber(LINE_ITEM_TABLE);
		int ordersInserted = insertedRowsNumber(ORDERS_TABLE);
		
		BasicDBObject pk_partsup = new BasicDBObject();
		pk_partsup.append("rows.ps_pk", true);
		pk_partsup.append("rows.ps_sk", true);
		BasicDBList partSuppRows = getListOfProjectedRows(PART_SUPP_TABLE, pk_partsup);
		
		document.put(INSERTED_ATTR, insertedRows + LINE_ITEM_NUM_INSERTS);
		for(int index = 1; index <= LINE_ITEM_NUM_INSERTS; index++) {
			BasicDBObject partSupp = (BasicDBObject) randomGenerator.getRandomItem(partSuppRows);
			rows.add(lineItem(index + insertedRows, ordersInserted, partSupp));
		}
		
		insertsTime += MongoHelper.executeInsertMeasuringTime(normalizedCollection, document);
	}

	private DBObject lineItem(int index, int ordersInserted, BasicDBObject partSupp) {
		
		BasicDBObject lineItem = new BasicDBObject();
		lineItem.append("l_ok", randomGenerator.randomInt(1, ordersInserted));
		lineItem.append("l_pk", partSupp.get("ps_pk"));
		lineItem.append("l_sk", partSupp.get("ps_sk"));
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
	
}
