package cbde.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class OracleHelper {

	private OracleHelper() { }

	public static ArrayList<ArrayList<String>> getColumns(Connection connection, String tableName, ArrayList<String> columns) {
		
		String fields = "";
		for (int i = 0; i < columns.size() - 1; i++) {
			fields += columns.get(i) + ", ";
		}
		if (columns.size() > 0) fields += columns.get(columns.size() - 1);
		
		ArrayList<ArrayList<String>> res = new ArrayList<ArrayList<String>>();
		
		try {
			Statement statement = connection.createStatement();
			ResultSet queryResult = statement.executeQuery("SELECT " + fields + " FROM " + tableName);
			ResultSetMetaData metadata = queryResult.getMetaData();
			
			while (queryResult.next()) {
				for (int index = 1; index <= metadata.getColumnCount(); index++) {
					res.get(index - 1).add(queryResult.getString(index -1));
				}
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return res;
	}
	
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
		System.out.println();
		
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
	
	public static int insertedRowsNumber(Connection connection, String tableName) throws SQLException {

		Statement statement = connection.createStatement();
		
		ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM " + tableName);
		result.next();
		int res = result.getInt(1);
		result.close();
		
	    return res;
	}

	public static void truncateTable(Connection connection, String tableName) throws SQLException {
		
		String query = "TRUNCATE TABLE " + tableName;
		
		Statement statement = connection.createStatement();
		statement.execute(query);
		statement.close();
	}
	
}
