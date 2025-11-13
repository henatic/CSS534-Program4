import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.*;

/**
 * CSS534 HW4 Based on Collin Gordon's BFS on 9/26/18
 */
public class ShortestPath {
    public static void main(String[] args) {
	// validate arguments.
	if ( args.length < 3 ) {
            System.out.println("Usage: filename source destinatioin");
            System.exit(-1);
	}

	// start Sparks and read a given input file
	String inputFile = args[0];
        SparkConf conf = new SparkConf( ).setAppName( "BFS-based Shortest Path Search" );
        JavaSparkContext jsc = new JavaSparkContext( conf );
	JavaRDD<String> lines = jsc.textFile( inputFile );

	// now start a timer
	System.err.println( "Timer got started." );
	long startTime = System.currentTimeMillis();

	// define two global variables
	LongAccumulator active = jsc.sc( ).longAccumulator( );            // # active nodes
	final Broadcast<String> sourceId = jsc.broadcast( args[1] );      // source node id
	final Broadcast<String> destinationId = jsc.broadcast( args[2] ); // dest node id

	// create the initial network information
	JavaPairRDD<String, Data> network = lines.mapToPair( line -> {
		// identify each node name
		String[] tokens = line.split("=");
		String node = tokens[0];

		// create a list of (neighbor_node, distance) pairs
		String[] nbrNodeDistPairs  = tokens[1].split(";");
		List<Tuple2<String,Integer>> neighbors = new ArrayList<Tuple2<String,Integer>>( );
		for ( int i = 0; i < nbrNodeDistPairs.length; i++ ) {
		    String[] pair = nbrNodeDistPairs[i].split(",");
		    Tuple2<String,Integer> tuple = new Tuple2<>( pair[0], Integer.valueOf( pair[1] ) );
		    neighbors.add( tuple );
		}

		// initialize the node's attributes
		Integer distance = Integer.MAX_VALUE;
		String status = "INACTIVE";
		if ( node.equals( sourceId.value( ) ) ) {
		    distance = 0;
		    status = "ACTIVE";
		    active.add( 1 );
		} 
		
		// return each node's information
		return new Tuple2<>( node, new Data( neighbors, distance, distance, status ) );
	    });

	// just for debugging
	System.err.println( "Initial Network:" );
	network.collect( );

	int steps = 0;
        // Start while loop if there is an active vertex in the network
        while (network.filter(d -> d._2().status.equals("ACTIVE")).count() > 0) {
            JavaPairRDD<String, Data> propagatedNetwork = network.flatMapToPair(vertex -> {
                // If a vertex is “ACTIVE”, create Tuple2( neighbor, new Data( … ) ) for
                // each neighbor where Data should include a new distance to this neighbor.
                // Add each Tuple2 to a list. Don’t forget this vertex itself back to the
                // list. Return all the list items.
            });

            network = propagatedNetwork.reduceByKey((k1, k2) -> {
                // For each key, (i.e., each vertex), find the shortest distance and
                // update this vertex’ Data attribute.
            });

            network = network.mapValues(value -> {
                // If a vertex’ new distance is shorter than prev, activate this vertex
                // status and replace prev with the new distance.
            });

	    // just for debugging
            System.err.println( "\nUpdated Network: " + ( steps++ ) );
            network.collect( );
        }

	    
	// get the distance from source to destination
	Tuple2<String, Data> destNode  = network.filter( entry -> {
                String myNode = entry._1( );
                return myNode.equals( destinationId.value( ) );
            } ).collect( ).iterator( ).next( );
	System.err.println( "from " + sourceId.value( ) + " to " + destNode._1( ) +
	                    " takes distance = " + destNode._2( ).distance );

        // stop the timer
        long endTime = System.currentTimeMillis();
        System.err.println("Elapsed Time: " + (endTime - startTime));

    }
}
