package cbde.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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
	}
	
	public void regionInserts() throws SQLException {
		
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
