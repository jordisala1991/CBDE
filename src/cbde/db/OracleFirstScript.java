package cbde.db;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OracleFirstScript {
	
	private Connection connection;
	private RandomGenerator randomGenerator;

	private static final String URL = "jdbc:oracle:thin:@oraclefib.fib.upc.es:1521:ORABD";
	private static final String USERNAME = "daniel.llamazares";
	private static final String PASSWORD = "DB021091";
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
	
	public OracleFirstScript() throws SQLException {
	
		connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
		randomGenerator = new RandomGenerator();
	}
	
	public void cleanTables() throws SQLException {
	
		OracleHelper.truncateTable(connection, REGION_TABLE);
		OracleHelper.truncateTable(connection, NATION_TABLE);
		OracleHelper.truncateTable(connection, SUPPLIER_TABLE);
		OracleHelper.truncateTable(connection, CUSTOMER_TABLE);
		OracleHelper.truncateTable(connection, PART_TABLE);
		OracleHelper.truncateTable(connection, ORDERS_TABLE);
		OracleHelper.truncateTable(connection, PART_SUPP_TABLE);
		OracleHelper.truncateTable(connection, LINE_ITEM_TABLE);
	}
	
	public void executeQuerys() throws SQLException {
		
		firstQuery();
		secondQuery();
		thirdQuery();
		fourthQuery();
	}

	public void firstQuery() throws SQLException {
		
		String query = 
			"SELECT " +
			"l_returnflag, l_linestatus, " +
			"sum(l_quantity) as sum_qty, " +
			"sum(l_extendedprice) as sum_base_price, " +
			"sum(l_extendedprice*(1-l_discount)) as sum_disc_price, " +
			"sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, " +
			"avg(l_quantity) as avg_qty, " +
			"avg(l_extendedprice) as avg_price, " +
			"avg(l_discount) as avg_disc, " +
			"count(*) as count_order " +
			"FROM lineitem " +
			"WHERE l_shipdate <= ? " +
			"GROUP BY l_returnflag, l_linestatus " +
			"ORDER BY l_returnflag, l_linestatus";
		
		PreparedStatement preparedStatement = connection.prepareStatement(query);
		//TODO: this is not the correct date, fetch from lineitem
		preparedStatement.setDate(1, new Date(new java.util.Date().getTime())); 
		
		ResultSet result = OracleHelper.executeQueryMeasuringTime(preparedStatement);
		OracleHelper.showQueryResult(result);
	}
	
	private void secondQuery() throws SQLException {
		
		String query = 
			"SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment " +
			"FROM part, supplier, partsupp, nation, region " +
			"WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = ? " +
			"AND p_type like ? AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey " +
			"AND r_name = ? AND ps_supplycost = (" +
			"SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey " +
			"AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey " +
			"AND n_regionkey = r_regionkey AND r_name = ?) " +
			"ORDER BY s_acctbal desc, n_name, s_name, p_partkey";
			
		PreparedStatement preparedStatement = connection.prepareStatement(query);
		//TODO: this is not the correct
		preparedStatement.setInt(1, randomGenerator.randomInt(4));
		preparedStatement.setString(2, randomGenerator.randomString(4));
		String region = randomGenerator.randomString(4);
		preparedStatement.setString(3, region);
		preparedStatement.setString(4, region);
		
		ResultSet result = OracleHelper.executeQueryMeasuringTime(preparedStatement);
		OracleHelper.showQueryResult(result);
	}

	private void thirdQuery() throws SQLException {
		
		String query = 
			"SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority " +
			"FROM customer, orders, lineitem " +
			"WHERE c_mktsegment = ? AND c_custkey = o_custkey AND l_orderkey = o_orderkey " +
			"AND o_orderdate < ? AND l_shipdate > ? " +
			"GROUP BY l_orderkey, o_orderdate, o_shippriority " +
			"ORDER BY revenue desc, o_orderdate";
			
		PreparedStatement preparedStatement = connection.prepareStatement(query);
		//TODO: this is not the correct
		preparedStatement.setString(1, randomGenerator.randomString(32));
		preparedStatement.setDate(2, new Date(new java.util.Date().getTime()));
		preparedStatement.setDate(3, new Date(new java.util.Date().getTime())); 
		
		ResultSet result = OracleHelper.executeQueryMeasuringTime(preparedStatement);
		OracleHelper.showQueryResult(result);
	}
	
	private void fourthQuery() throws SQLException {
		
		String query = 
				"SELECT " +
				"l_returnflag, l_linestatus, " +
				"sum(l_quantity) as sum_qty, " +
				"sum(l_extendedprice) as sum_base_price, " +
				"sum(l_extendedprice*(1-l_discount)) as sum_disc_price, " +
				"sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, " +
				"avg(l_quantity) as avg_qty, " +
				"avg(l_extendedprice) as avg_price, " +
				"avg(l_discount) as avg_disc, " +
				"count(*) as count_order " +
				"FROM lineitem " +
				"WHERE l_shipdate <= ? " +
				"GROUP BY l_returnflag, l_linestatus " +
				"ORDER BY l_returnflag, l_linestatus";
			
			PreparedStatement preparedStatement = connection.prepareStatement(query);
			//TODO: this is not the correct date, fetch from lineitem
			preparedStatement.setDate(1, new Date(new java.util.Date().getTime())); 
			
			ResultSet result = OracleHelper.executeQueryMeasuringTime(preparedStatement);
			OracleHelper.showQueryResult(result);
		
	}

	public void randomInserts() throws SQLException {
		
		regionInserts();
		nationInserts();
		supplierInserts();
		customerInserts();
		partInserts();
		ordersInserts();
		//partSuppInserts();
		//lineItemInserts();
	}
	
	private void regionInserts() throws SQLException {
		
		if (OracleHelper.insertedRowsNumber(connection, REGION_TABLE) == 0) { 
			String regionInsert = "INSERT INTO region (R_RegionKey, R_Name, R_Comment) VALUES (?, ?, ?)";
			
			PreparedStatement preparedStatement = connection.prepareStatement(regionInsert);
			for (int index = 1; index <= REGION_NUM_INSERTS; index++) {
				preparedStatement.setInt(1, index);
				preparedStatement.setString(2, randomGenerator.randomString(32));
				preparedStatement.setString(3, randomGenerator.randomString(80));
				preparedStatement.addBatch();
			}
			preparedStatement.executeBatch();
			preparedStatement.close();
		}
	}
	
	private void nationInserts() throws SQLException {
		
		if (OracleHelper.insertedRowsNumber(connection, NATION_TABLE) == 0) { 		
			String nationInsert = "INSERT INTO nation (N_NationKey, N_Name, N_RegionKey, N_Comment) VALUES (?, ?, ?, ?)";
			int regionsInserted = OracleHelper.insertedRowsNumber(connection, REGION_TABLE);
			
			PreparedStatement preparedStatement = connection.prepareStatement(nationInsert);
			for (int index = 1; index <= NATION_NUM_INSERTS; index++) {
				preparedStatement.setInt(1, index);
				preparedStatement.setString(2, randomGenerator.randomString(32));
				preparedStatement.setInt(3, randomGenerator.randomInt(1, regionsInserted));
				preparedStatement.setString(4, randomGenerator.randomString(80));
				preparedStatement.addBatch();
			}
			preparedStatement.executeBatch();
			preparedStatement.close();
		}
	}
	
	private void supplierInserts() throws SQLException {
		
		String supplierInsert = "INSERT INTO supplier (S_SuppKey, S_Name, S_Address, S_NationKey, S_Phone, S_AcctBal, S_Comment) VALUES (?, ?, ?, ?, ?, ?, ?)";
		int nationsInserted = OracleHelper.insertedRowsNumber(connection, NATION_TABLE);
		int insertedRows = OracleHelper.insertedRowsNumber(connection, SUPPLIER_TABLE);
		
		for (int index = 1; index <= SUPPLIER_NUM_INSERTS; index++) {
			PreparedStatement preparedStatement = connection.prepareStatement(supplierInsert);
			preparedStatement.setInt(1, index + insertedRows);			
			preparedStatement.setString(2, randomGenerator.randomString(32));
			preparedStatement.setString(3, randomGenerator.randomString(32));
			preparedStatement.setInt(4, randomGenerator.randomInt(1, nationsInserted));
			preparedStatement.setString(5, randomGenerator.randomString(9));
			preparedStatement.setInt(6, randomGenerator.randomInt(7));
			preparedStatement.setString(7, randomGenerator.randomString(53));
			preparedStatement.executeUpdate();
			preparedStatement.close();
		}
	}
	
	private void customerInserts() throws SQLException {
		
		String customerInsert = "INSERT INTO customer (C_CustKey, C_Name, C_Address, C_NationKey, C_Phone, C_AcctBal, C_MktSegment, C_Comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
		int nationsInserted = OracleHelper.insertedRowsNumber(connection, NATION_TABLE);
		int insertedRows = OracleHelper.insertedRowsNumber(connection, CUSTOMER_TABLE);
		
		for (int index = 1; index <= CUSTOMER_NUM_INSERTS; index++) {
			PreparedStatement preparedStatement = connection.prepareStatement(customerInsert);
			preparedStatement.setInt(1, index + insertedRows);			
			preparedStatement.setString(2, randomGenerator.randomString(32));
			preparedStatement.setString(3, randomGenerator.randomString(32));
			preparedStatement.setInt(4, randomGenerator.randomInt(1, nationsInserted));
			preparedStatement.setString(5, randomGenerator.randomString(32));
			preparedStatement.setInt(6, randomGenerator.randomInt(7));
			preparedStatement.setString(7, randomGenerator.randomString(32));
			preparedStatement.setString(8, randomGenerator.randomString(60));
			preparedStatement.executeUpdate();
			preparedStatement.close();
		}
	}
	
	private void partInserts() throws SQLException {
		
		String partInsert = "INSERT INTO part (P_PartKey, P_Name, P_Mfgr, P_Brand, P_Type, P_Size, P_Container, P_RetailPrice, P_Comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
		int insertedRows = OracleHelper.insertedRowsNumber(connection, PART_TABLE);
		
		for (int index = 1; index <= PART_NUM_INSERTS; index++) {
			PreparedStatement preparedStatement = connection.prepareStatement(partInsert);
			preparedStatement.setInt(1, index + insertedRows);			
			preparedStatement.setString(2, randomGenerator.randomString(32));
			preparedStatement.setString(3, randomGenerator.randomString(32));
			preparedStatement.setString(4, randomGenerator.randomString(32));
			preparedStatement.setString(5, randomGenerator.randomString(32));
			preparedStatement.setInt(6, randomGenerator.randomInt(4));
			preparedStatement.setString(7, randomGenerator.randomString(32));
			preparedStatement.setInt(8, randomGenerator.randomInt(7));
			preparedStatement.setString(9, randomGenerator.randomString(32));
			preparedStatement.executeUpdate();
			preparedStatement.close();
		}
	}
	
	private void ordersInserts() throws SQLException {
		
		String partInsert = "INSERT INTO orders (O_OrderKey, O_CustKey, O_OrderStatus, O_TotalPrice, O_OrderDate, O_OrderPriority, O_Clerk, O_ShipPriority, O_Comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
		int customersInserted = OracleHelper.insertedRowsNumber(connection, CUSTOMER_TABLE);
		int insertedRows = OracleHelper.insertedRowsNumber(connection, ORDERS_TABLE);
		
		for (int index = 1; index <= ORDERS_NUM_INSERTS; index++) {
			PreparedStatement preparedStatement = connection.prepareStatement(partInsert);
			preparedStatement.setInt(1, index + insertedRows);			
			preparedStatement.setInt(2, randomGenerator.randomInt(1, customersInserted));
			preparedStatement.setString(3, randomGenerator.randomString(32));
			preparedStatement.setInt(4, randomGenerator.randomInt(7));
			Date sqlDate = new Date(randomGenerator.randomDate().getTime());
			preparedStatement.setDate(5, sqlDate);
			preparedStatement.setString(6, randomGenerator.randomString(8));
			preparedStatement.setString(7, randomGenerator.randomString(32));
			preparedStatement.setInt(8, randomGenerator.randomInt(4));
			preparedStatement.setString(9, randomGenerator.randomString(40));
			preparedStatement.executeUpdate();
			preparedStatement.close();
		}
	}
	
}
