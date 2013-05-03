package cbde.db;

import java.net.UnknownHostException;
import java.sql.SQLException;

public class Main {

 	public static void main(String[] args) throws UnknownHostException, SQLException {
 	
 		/*MongoNormalizedScript mongoScript = new MongoNormalizedScript();
 		mongoScript.deleteCollection();
 		mongoScript.randomInserts();*/
 		OracleFirstScript ofc = new OracleFirstScript();
 		ofc.randomInserts();
	}

}
