package cbde.db.oracle;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class OracleHelper {

	private OracleHelper() { }

	public static ArrayList<ArrayList<String>> getColumns(Connection connection, String tableName, ArrayList<String> columns) throws SQLException {
		
		String fields = "";
		fields += columns.get(0);
		for (int index = 1; index < columns.size(); index++) {
			fields += ", " + columns.get(index);
		}
		
		ArrayList<ArrayList<String>> rows = new ArrayList<ArrayList<String>>();
		
		Statement statement = connection.createStatement();
		ResultSet queryResult = statement.executeQuery("SELECT " + fields + " FROM " + tableName);
		
		while (queryResult.next()) {
			ArrayList<String> row = new ArrayList<String>();
			for (int index = 1; index <= columns.size(); index++) {
				row.add(queryResult.getString(index));
			}
			rows.add(row);
		}
		statement.close();
		queryResult.close();
		
		return rows;
	}
	
	public static void showQueryResult(ResultSet queryResult) throws SQLException {
		
		ResultSetMetaData metadata = queryResult.getMetaData();
		for (int index = 1; index <= metadata.getColumnCount(); ++index) {			
			System.out.print(metadata.getColumnLabel(index) + "\t");
		}
		System.out.println();
		
		int rowCount = 0;
		while (queryResult.next() && rowCount < 10) {
			for (int index = 1; index <= metadata.getColumnCount(); index++) {
				System.out.print(queryResult.getString(index) + "\t");
			}
			System.out.println();
			++rowCount;
		}
		queryResult.last();
		System.out.println("Total rows: " + queryResult.getRow());
		System.out.println("------------------------------------");
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
	
	public static double executeInsertMeasuringTime(PreparedStatement preparedStatement) throws SQLException {
		
		long startTime = System.nanoTime();
		preparedStatement.executeBatch();
		long endTime = System.nanoTime();
		
		preparedStatement.close();
		
		return (endTime - startTime) / 1.0E09;
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
	
	public static Date convertToSQLDate(java.util.Date date) {
		
		return new Date(date.getTime());
	}
	
}
