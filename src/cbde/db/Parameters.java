package cbde.db;

import java.util.Map;

public class Parameters {

	private static String databaseName;
	private static int method;
	
	private static final String DATABASE_NAME = "database";
	private static final String METHOD = "method";
	
	public static final String ORACLE = "oracle";
	public static final int FIRST_METHOD = 1;

	private Parameters() { }

	public static void initialize(Map<String, String> configuration) {

		databaseName = configuration.get(DATABASE_NAME);
		method = Integer.valueOf(configuration.get(METHOD));
	}

	public static String getDatabaseName() {
		return databaseName;
	}

	public static int getMethod() {
		return method;
	}

}
