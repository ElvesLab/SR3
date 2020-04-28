package pastry_replica;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.*;
import java.util.*;

import javax.xml.ws.Endpoint;

import rice.environment.Environment;
import rice.p2p.commonapi.NodeHandle;
import rice.pastry.*;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.pastry.transport.TransportPastryNodeFactory;

public class pastry_main {
	/**
	 * This class is used to design the main class for the whole test, and start the publish task via nodes
	 */
	  /**
	   * this will keep track of our Scribe applications
	   */
	  Vector<MultipleNodeApplication> apps = new Vector<MultipleNodeApplication>();
	  
  public pastry_main(int bindport, InetSocketAddress bootaddress, int numNodes, Environment env) throws Exception{
	  //Generate the nodeIds randomly
	  NodeIdFactory nidFactory = new RandomNodeIdFactory(env);
	  //Construct the PastryNodeFactory, here we use rice.psastry.socket
	  PastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory, bindport, env);
	  
	  /** single create a node
	   * 
	  //construct a new node
	  PastryNode node = factory.newNode();
	  //construct a new scribe application
	  
	  TreeNodeApplication app = new TreeNodeApplication(bindport, node);
	  
	  node.boot(bootaddress);
	  
	  synchronized(node){
		  while(!node.isReady() && !node.joinFailed()){
			  //delay if we don't busy-wait
			  node.wait(500);
			  //abort if we can't join
			  if(node.joinFailed()){
				  throw new IOException("Cannot join the Pastry ring. Reason:"+node.joinFailedReason());
			  }
		  }
	  }
	  System.out.println("Finished creating new node: "+node);
	 
	  app.subscribe();
	  env.getTimeSource().sleep(10000);
	
	  app.startPublishTask();
	  */
	  
	  long treeStartTime = System.currentTimeMillis();
	  // loop to construct the nodes/apps
	    for (int curNode = 0; curNode < numNodes; curNode++) {
	      // construct a new node
	      PastryNode node = factory.newNode();
	      
	      // construct a new scribe application
	      MultipleNodeApplication app = new MultipleNodeApplication(node);
	      apps.add(app);
	      
	      node.boot(bootaddress);
	      
	      // the node may require sending several messages to fully boot into the ring
	      synchronized(node) {
	        while(!node.isReady() && !node.joinFailed()) {
	          // delay so we don't busy-wait
	          node.wait(500);
	          
	          // abort if can't join
	          if (node.joinFailed()) {
	            throw new IOException("Could not join the FreePastry ring.  Reason:"+node.joinFailedReason()); 
	          		}
	        	}       
	      	}
	      	System.out.println("Finished creating new node: " + node);
	    	}
	    /*
	    long treeEndTime = System.currentTimeMillis() - treeStartTime;
	    FileWriter fw;
		try {
			fw = new FileWriter("D:/Eclipse workplace/TreeBuildTime.txt", true);
			BufferedWriter bw = new BufferedWriter(fw);
		    PrintWriter out = new PrintWriter(bw);			 
		    out.println( "Build the tree takes  " +treeEndTime );			
		    out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
	    // for the first app subscribe then start the publishtask
	    Iterator<MultipleNodeApplication> i = apps.iterator();
	    MultipleNodeApplication app = (MultipleNodeApplication) i.next();
	    app.subscribe();
	    env.getTimeSource().sleep(5000);
	    app.startPublishTask();
	    // for all the rest just subscribe
	    while (i.hasNext()) {
	      app = (MultipleNodeApplication) i.next();
	      app.subscribe();
	    }
	    
	 // now, print the tree
	    //env.getTimeSource().sleep(2000);
	   // printTree(apps);
  }
  
  /**
   * Note that this function only works because we have global knowledge. Doing
   * this in an actual distributed environment will take some more work.
   * 
   * @param apps Vector of the applicatoins.
   */
  public static void printTree(Vector<MultipleNodeApplication> apps) {
    // build a hashtable of the apps, keyed by nodehandle
    Hashtable<NodeHandle, MultipleNodeApplication> appTable = new Hashtable<NodeHandle, MultipleNodeApplication>();
    Iterator<MultipleNodeApplication> i = apps.iterator();
    while (i.hasNext()) {
    	MultipleNodeApplication app = (MultipleNodeApplication) i.next();
      appTable.put(app.endpoint.getLocalNodeHandle(), app);
    }
    NodeHandle seed = ((MultipleNodeApplication) apps.get(0)).endpoint
        .getLocalNodeHandle();

    // get the root
    NodeHandle root = getRoot(seed, appTable);

    // print the tree from the root down
    recursivelyPrintChildren(root, 0, appTable);
  }

  /**
   * Recursively crawl up the tree to find the root.
   */
  public static NodeHandle getRoot(NodeHandle seed, Hashtable<NodeHandle, MultipleNodeApplication> appTable) {
	  MultipleNodeApplication app = (MultipleNodeApplication) appTable.get(seed);
    if (app.isRoot())
      return seed;
    NodeHandle nextSeed = app.getParent();
    return getRoot(nextSeed, appTable);
  }

  /**
   * Print's self, then children.
   */
  public static void recursivelyPrintChildren(NodeHandle curNode,
      int recursionDepth, Hashtable<NodeHandle, MultipleNodeApplication> appTable) {
    // print self at appropriate tab level
    String s = "";
    for (int numTabs = 0; numTabs < recursionDepth; numTabs++) {
      s += "  ";
    }
    s += curNode.getId().toString();
    System.out.println(s);
    FileWriter fw;
	try {
		fw = new FileWriter("D:\\Eclipse workplace\\pastry_replica_test\\TreeStructure.txt", true);
		BufferedWriter bw = new BufferedWriter(fw);
	    PrintWriter out = new PrintWriter(bw);
		 
	    out.println(s);
		
	    out.close();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

    // recursively print all children
    MultipleNodeApplication app = (MultipleNodeApplication) appTable.get(curNode);
    NodeHandle[] children = app.getChildren();
    for (int curChild = 0; curChild < children.length; curChild++) {
      recursivelyPrintChildren(children[curChild], recursionDepth + 1, appTable);
    }
  }

  public static void main(String[] args) throws Exception{
	  Environment env = new Environment("src/new");
	  env.getParameters().setString("nat_search_plicy", "never");
	  
	  try{
		  // locally used port
		  int bindport = Integer.parseInt(args[0]);
		  
		  // build the bootaddress from the command line args
		  InetAddress bootaddr = InetAddress.getByName(args[1]);
		  int bootport = Integer.parseInt(args[2]);
		  InetSocketAddress bootaddress = new InetSocketAddress(bootaddr, bootport);
		// the port to use locally
	      int numNodes = Integer.parseInt(args[3]);
		  //launch the node
	      pastry_main dt = new pastry_main(bindport, bootaddress, numNodes, env);
		  
	  } catch (Exception e){
		  System.out.println("Usage:");
		  System.out.println("java [-cp FreePastry-<version>.jar] TreeTestMain");
		  System.out.println("From TreeTestMain 9001 xx.xxx.xxx.xx 9001 xx");
		  throw e;
	  }
  }
}
