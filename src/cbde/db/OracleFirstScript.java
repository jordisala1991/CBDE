package cbde.db;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class OracleFirstScript {
	
	private Connection connection;
	private RandomGenerator randomGenerator;

	private static final String URL = "jdbc:oracle:thin:@oraclefib.fib.upc.es:1521:ORABD";
	private static final String USERNAME = "daniel.llamazares";
	private static final String PASSWORD = "DB021091";
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
	
	public void executeQuerys() throws SQLException {
		
		firstQuery();
	}
	
	public void firstQuery() throws SQLException {
		
		String query = "SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, " +
			"sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as " +
			"sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, " +
			"avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) " +
			"as avg_disc, count(*) as count_order FROM lineitem WHERE l_shipdate <= ? " +
			"GROUP BY l_returnflag, l_linestatus " +
			"ORDER BY l_returnflag, l_linestatus;";
		
		PreparedStatement preparedStatement = connection.prepareStatement(query);
		preparedStatement.setDate(1, new Date(new java.util.Date().getTime()));
		ResultSet result = preparedStatement.executeQuery();
		
	}

	public void randomInserts() throws SQLException {
		
		regionInserts();
		nationInserts();
		supplierInserts();
		customerInserts();
		partInserts();
		ordersInserts();
		partSuppInserts();
		lineItemInserts();
	}
	
	private int insertedRowsNumber(String tableName) {

		Statement statement = null;
		ResultSet result = null;
		int res = 0;
		try {
			statement = connection.createStatement();
		    result = statement.executeQuery("SELECT COUNT(*) FROM " + tableName);
		    result.next();
		    res = result.getInt(1);
		    result.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	    return res;
	}	
	
	private void regionInserts() throws SQLException {
		
		if (insertedRowsNumber("region") == 0) { 
			String regionInsert = "INSERT INTO region" + "(R_RegionKey, R_Name, R_Comment) VALUES" + "(?, ?, ?)";
			
			for (int index = 1; index <= REGION_NUM_INSERTS; index++) {
				PreparedStatement preparedStatement = connection.prepareStatement(regionInsert);
				preparedStatement.setInt(1, index);
				preparedStatement.setString(2, randomGenerator.randomString(32));
				preparedStatement.setString(3, randomGenerator.randomString(80));
				preparedStatement.executeUpdate();
			}
		}
	}
	
	private void nationInserts() throws SQLException {
		
		if (insertedRowsNumber("nation") == 0) { 		
			String nationInsert = "INSERT INTO nation" + "(N_NationKey, N_Name, N_RegionKey, N_Comment) VALUES" + "(?, ?, ?, ?)";
			int regionsInserted = insertedRowsNumber("region");
			
			for (int index = 1; index <= NATION_NUM_INSERTS; index++) {
				PreparedStatement preparedStatement = connection.prepareStatement(nationInsert);
				preparedStatement.setInt(1, index);
				preparedStatement.setString(2, randomGenerator.randomString(32));
				preparedStatement.setInt(3, randomGenerator.randomInt(1, regionsInserted));
				preparedStatement.setString(4, randomGenerator.randomString(80));
				preparedStatement.executeUpdate();
			}
		}
	}
	
	private void supplierInserts() throws SQLException {
		
		String supplierInsert = "INSERT INTO supplier" + "(S_SuppKey, S_Name, S_Address, S_NationKey, S_Phone, S_AcctBal, S_Comment) VALUES" + "(?, ?, ?, ?, ?, ?, ?)";
		int nationsInserted = insertedRowsNumber("nation");
		int insertedRows = insertedRowsNumber("supplier");
		
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
		}
	}
	
	private void customerInserts() throws SQLException {
		
		String customerInsert = "INSERT INTO customer" + "(C_CustKey, C_Name, C_Address, C_NationKey, C_Phone, C_AcctBal, C_MktSegment, C_Comment) VALUES" + "(?, ?, ?, ?, ?, ?, ?, ?)";
		int nationsInserted = insertedRowsNumber("nation");
		int insertedRows = insertedRowsNumber("customer");
		
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
		}
	}
	
}
