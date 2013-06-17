package cbde.db;

import java.sql.SQLException;
import java.util.ArrayList;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import cbde.db.oracle.OracleHelper;

public class neo4j {
	
	private static final String DB_PATH = "neo4j";
	
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
	
	private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
	
	
	public void randomInserts() throws SQLException {
		
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		registerShutdownHook( graphDb );
		
		Transaction tx = graphDb.beginTx();
		try
		{
			insertsTime = 0;
			regions = regionInserts();
			nations = nationInserts(regions);
			System.out.println("Inserts time: " + insertsTime + " seconds");
			System.out.println("------------------------------------");		
			
		    tx.success();
		}
		finally
		{
		    tx.finish();
		}

	}
	
	private ArrayList<Node> regionInserts() throws SQLException {
		
		regions = new ArrayList<Node>();
		for (int index = 1; index <= REGION_NUM_INSERTS; index++) {
			Node region = graphDb.createNode();
			region.setProperty("regionKey", index);
			region.setProperty("name", randomGenerator.randomString(32));
			region.setProperty("comment", randomGenerator.randomString(80));
			
			regions.add(region);
		}
		//insertsTime += Neo4jHelper.executeInsertMeasuringTime(???);
		
		return regions;
	}
	
	private ArrayList<Node> nationInserts(ArrayList<Node> regions) throws SQLException {
		
		nations = new ArrayList<Node>();
		for (int index = 1; index <= NATION_NUM_INSERTS; index++) {
			Node region = (Node) randomGenerator.getRandomItem(regions);
			
			Node nation = graphDb.createNode();
			nation.setProperty("nationKey", index);
			nation.setProperty("name", randomGenerator.randomString(32));
			nation.setProperty("regionKey", region.getProperty("regionKey"));
			nation.setProperty("comment", randomGenerator.randomString(80));
			
			region.createRelationshipTo(nation, RelTypes.REGION_NATION);
			nations.add(nation);
		}
		
		return nations;
	}
	
	/*
	public static void main(String[] args) throws Exception {

		GraphDatabaseService graphDb;
		Node firstNode;
		Node secondNode;
		Relationship relationship;
		
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase("new");
		registerShutdownHook( graphDb );
		
		Transaction tx = graphDb.beginTx();
		try
		{
		    // Updating operations go here
			firstNode = graphDb.createNode();
			firstNode.setProperty("message", "Hello, ");
			secondNode = graphDb.createNode();
			secondNode.setProperty("message", "World!");
			 
			relationship = firstNode.createRelationshipTo(secondNode, RelTypes.KNOWS);
			relationship.setProperty("message", "brave Neo4j ");			
			
		    tx.success();
		}
		finally
		{
		    tx.finish();
		}
		
		System.out.print( firstNode.getProperty( "message" ) );
		System.out.print( relationship.getProperty( "message" ) );
		System.out.print( secondNode.getProperty( "message" ) );

	}*/
	

}
