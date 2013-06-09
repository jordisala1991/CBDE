package cbde.db;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
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
		}
		else {
			if (Parameters.getMethod() == Parameters.FIRST_METHOD) {
				MongoNormalizedScript mongoScript = new MongoNormalizedScript();
				mongoScript.deleteCollection();
				mongoScript.randomInserts();
				mongoScript.executeQuerys();
			}
			else {
				MongoDenormalizedScript mongoScript = new MongoDenormalizedScript();
				mongoScript.randomInserts();
				mongoScript.deleteAllCollections();
			}
		}
	}
 	
 	@SuppressWarnings("unchecked")
	private static void readParameters() throws FileNotFoundException, YamlException {

		YamlReader reader = new YamlReader(new FileReader(PARAMETERS_FILENAME));

		Map<String, String> configuration = (Map<String, String>) reader.read();
		Parameters.initialize(configuration);
	}

}
