package cbde.db;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import cbde.db.mongo.MongoDenormalizedScript;
import cbde.db.mongo.MongoNormalizedScript;
import cbde.db.neo4j.Neo4jScript;
import cbde.db.oracle.OracleFirstScript;
import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;

public class Main {

 	private static final String PARAMETERS_FILENAME = "parameters.yml";

	public static void main(String[] args) throws Exception {
 	
		readParameters();
		
		if (Parameters.getDatabaseName().equals(Parameters.ORACLE)) {
			OracleFirstScript ofc = new OracleFirstScript();
			ofc.cleanTables();
			ofc.randomInserts();
			ofc.executeQuerys();
			ofc.randomInserts();
			ofc.executeQuerys();
			ofc.cleanTables();
		}
		else if (Parameters.getDatabaseName().equals(Parameters.MONGODB)) {
			if (Parameters.getMethod() == Parameters.FIRST_METHOD) {
				MongoNormalizedScript mongoScript = new MongoNormalizedScript();
				mongoScript.deleteCollection();
				mongoScript.randomInserts();
				mongoScript.randomInserts();
				mongoScript.deleteCollection();
			}
			else {
				MongoDenormalizedScript mongoScript = new MongoDenormalizedScript();
				mongoScript.deleteAllCollections();
				mongoScript.randomInserts(0);
				mongoScript.randomInserts(1);
				mongoScript.deleteAllCollections();
			}
		}
		else if (Parameters.getDatabaseName().equals(Parameters.NEO4J)){
			Neo4jScript neo4j = new Neo4jScript();
			neo4j.randomInserts();
			neo4j.shutdownDatabase();
		}
	}
 	
 	@SuppressWarnings("unchecked")
	private static void readParameters() throws FileNotFoundException, YamlException {

		YamlReader reader = new YamlReader(new FileReader(PARAMETERS_FILENAME));

		Map<String, String> configuration = (Map<String, String>) reader.read();
		Parameters.initialize(configuration);
	}

}
