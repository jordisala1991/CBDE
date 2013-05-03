package cbde.db;

import java.sql.Connection;
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

	public void randomInserts() throws SQLException {
		
		regionInserts();
		nationInserts();
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
				preparedStatement .executeUpdate();
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
	
}
