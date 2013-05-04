package cbde.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class OracleHelper {

	public static void showQueryResult(ResultSet queryResult) throws SQLException {
		
		ResultSetMetaData metadata = queryResult.getMetaData();
		for (int index = 1; index <= metadata.getColumnCount(); ++index) {			
			System.out.print(metadata.getColumnLabel(index) + "\t");
		}
		System.out.println();
		
		while (queryResult.next()) {
			for (int index = 1; index <= metadata.getColumnCount(); index++) {
				System.out.print(queryResult.getString(index) + "\t");
			}
			System.out.println();
		}
		
		queryResult.close();
	}
	
	public static ResultSet executeQueryMeasuringTime(PreparedStatement preparedStatement) throws SQLException {
		
		long startTime = System.nanoTime();
		ResultSet result = preparedStatement.executeQuery();
		long endTime = System.nanoTime();

		double seconds = (endTime - startTime) / 1.0E09;
		System.out.println("Query Time: " + seconds + " seconds");
		
		return result;
	}

	public static void truncateTable(Connection connection, String tableName) throws SQLException {
		
		String query = "TRUNCATE TABLE " + tableName;
		
		Statement statement = connection.createStatement();
		statement.execute(query);
		statement.close();
	}
	
}
