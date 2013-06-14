package cbde.db;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class neo4j {
	
	private static final String DB_PATH = "neo4j";
	
	private static enum RelTypes implements RelationshipType
	{
		KNOWS
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
	
	public static void main(String[] args) throws Exception {

		GraphDatabaseService graphDb;
		Node firstNode;
		Node secondNode;
		Relationship relationship;
		
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
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

	}
	

}
