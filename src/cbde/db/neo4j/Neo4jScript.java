package cbde.db.neo4j;

import java.util.ArrayList;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import cbde.db.RandomGenerator;

public class Neo4jScript {
	
	private static final String DB_PATH = "neo4jdb";
	
	private GraphDatabaseService graphDb;
	
	private RandomGenerator randomGenerator;
	private double insertsTime;
	
	private ArrayList<Node> regions;
	private ArrayList<Node> nations;
	private ArrayList<Node> customer;
	private ArrayList<Node> orders;
	private ArrayList<Node> supplier;
	private ArrayList<Node> part;
	private ArrayList<Node> partsupp;
	
	private static final int REGION_NUM_INSERTS = 5;
	private static final int NATION_NUM_INSERTS = 25;
	private static final int SUPPLIER_NUM_INSERTS = 33;
	private static final int CUSTOMER_NUM_INSERTS = 500;
	private static final int PART_NUM_INSERTS = 666;
	private static final int ORDERS_NUM_INSERTS = 5000;
	private static final int PART_SUPP_NUM_INSERTS = 2666;
	private static final int LINE_ITEM_NUM_INSERTS = 20000;
	
	public Neo4jScript() {
		
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		randomGenerator = new RandomGenerator();
	}
	
	private static enum RelTypes implements RelationshipType
	{
		REGION_NATION,
		NATION_SUPPLIER,
		NATION_CUSTOMER,
		CUSTOMER_ORDERS,
		ORDERS_LINEITEM,
		SUPPLIER_PARTSUPP,
		PART_PARTSUPP,
		PARTSUPP_LINEITEM_PARTKEY,
		PARTSUPP_LINEITEM_SUPPKEY,
	}
	
	public void shutdownDatabase() {
		graphDb.shutdown();
	}
	
	public void randomInserts() {
		
		Transaction tx = graphDb.beginTx();
		
		long startTime = System.nanoTime();
		regions = regionInserts();
		nations = nationInserts(regions);
		long endTime = System.nanoTime();
		
		insertsTime = (endTime - startTime) / 1.0E09;
		System.out.println("Inserts time: " + insertsTime + " seconds");
		System.out.println("------------------------------------");		
			
		tx.success();
		tx.finish();
	}
	
	private ArrayList<Node> regionInserts() {
		
		regions = new ArrayList<Node>();
		
		for (int index = 1; index <= REGION_NUM_INSERTS; index++) {
			Node region = graphDb.createNode();
			region.setProperty("regionKey", index);
			region.setProperty("name", randomGenerator.randomString(32));
			region.setProperty("comment", randomGenerator.randomString(80));
			
			regions.add(region);
		}
		
		return regions;
	}
	
	private ArrayList<Node> nationInserts(ArrayList<Node> regions) {
		
		nations = new ArrayList<Node>();
		
		for (int index = 1; index <= NATION_NUM_INSERTS; index++) {
			Node nation = graphDb.createNode();
			nation.setProperty("nationKey", index);
			nation.setProperty("name", randomGenerator.randomString(32));
			nation.setProperty("comment", randomGenerator.randomString(80));
			
			Node region = (Node) randomGenerator.getRandomItem(regions);
			nation.createRelationshipTo(region, RelTypes.REGION_NATION);
			nations.add(nation);
		}
		
		return nations;
	}

}
