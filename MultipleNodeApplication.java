package pastry_replica;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.Vector;
import java.io.PrintWriter;
import org.apache.commons.io.FilenameUtils;

import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.CancellableTask;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.scribe.Scribe;
import rice.p2p.scribe.ScribeClient ;
import rice.p2p.scribe.ScribeContent;
import rice.p2p.scribe.ScribeImpl;
import rice.p2p.scribe.Topic;
import rice.pastry.commonapi.PastryIdFactory;

public class MultipleNodeApplication implements ScribeClient, Application {
  
  CancellableTask publishTask;
  /**
   * My handle to a scribe impl
   */
  Scribe myScribe;
  // Here to give a default value to each node for delievr message
  int nodeValue = 0; // use for root to publish msg and aggregate all parts. use for root node
  int receivedPart = 0; //use for node who received part of file
  int sendReceivedPart =0; //use for node who has part then try to send to root or parent
  String contentPart = "Hi"; //note this string is used for the content of received part
  // current path is used for node to creat new folder
  //String currentPath = "D:\\Eclipse workplace\\pastry_replica_test";
  String currentPath = "/home/hxu017/Eclipse_workplace";
  //String currentPath = "/Users/hailuxu/Downloads/Research file/2020_Spring/pastry_replica/pastry_replica_test";
  
  TreeMap<Integer, List<Id>> part_map = new TreeMap<Integer, List<Id>>();
  
  List<String> TheList = new ArrayList<String>();
  
  // countNum to record the number of parts that root already collected
  int rootCollectedNum = 0;
  
  String appName; //Here is for the application name 
  int numParts = 512; // denote the kb data of a part, e.g., 1024kb = 1mb
  int numReplica = 2; // denote how many replicas for one part
  int numSpli =0; //use for root to record how many parts for a file
  int lengthPath = 0; //here is used for user to define the length of recovery path
  String latencyRequire;//here for define the latency requirement of app
  
  //---------------------------------------------------------------------
  // countNum to record the number of received value
  int countNum = 0;
  int countReplica = 0; //user for node to record how many replica it received
  
  int bindport =0;
  
  long startTime;
  long endTime;
  
  String [][] intArray = new String[200][2];
  String [] fileName = new String[10];
  // The topic this application is subscribing to
  Topic myTopic;
  Topic topic;
  
  Vector<String> vecData = new Vector<String>(2000, 0);
  
  /**
   * The Endpoint represents the underlieing node.  By making calls on the 
   * 
   * Endpoint, it assures that the message will be delivered to a MyApp on whichever
   * node the message is intended for.
   */
  protected Endpoint endpoint;
  
  public MultipleNodeApplication (Node node){
	  this.endpoint = node.buildEndpoint(this, "myinstance");
	  
	  myScribe = new ScribeImpl(node, "myScribeInstance");
	  myTopic = new Topic(new PastryIdFactory(node.getEnvironment()),"Tree Test1");
	  	 	  
	  System.out.println("myTopic= "+myTopic);
	  System.out.println("Node:"+endpoint.getId()+" NodeValue= "+nodeValue);
	  
	  endpoint.register();	  
	  //bindport= bindPort;
  }
  /**
   * subscribe to myTopic
   */
  public void subscribe() {
	  myScribe.subscribe(myTopic, this);
  }
  /**
   * start to publish message
 * @throws IOException 
 * @throws InterruptedException 
   */
  public void startPublishTask() throws IOException{
	  //publishTask = endpoint.scheduleMessage(new PublishContent(), 10000, 10000);
	  //if (this.myScribe.isRoot(myTopic)){
          
		  //sendMultiCast();
		  publishTask = endpoint.scheduleMessage(new PublishContent(), 20000, 30000);
		  System.out.println("---------------------------------------------------------------------------------------------------------");
		  System.out.println(endpoint.getId() + " :I am ROOT and start to split state and save to my childrens");
		  
		  
	  //}
  }
  
  /**
   * Deliver the message at beginning and also output information when node receive wanted message
   */
  @Override
public void deliver (Id id, Message message) {
	  if (message instanceof PublishContent){ //PublishContent can only be sent by ROOT, so here is the functions for root node.
		  
		  /* here we need to use root to split file into parts, and make replica for each part, then
		   * send the parts to different nodes, and have a table to record which node stores which part
		   * Besides, root will send msg to ask the following nodes to send part to it, so it can merge
		   */
		  //First, root node splits the file and make replica for splited parts
		  if (nodeValue ==0) {
			  
			  nodeValue++; //nadeValue is used for identify the first and second rounds for split and aggregate
			  
			  //byte numParts = 50; // denote the number of parts of a file
			  //int numReplica = 2; // denote how many replicas for one part
			  
			  NodeHandle[] childList = this.getChildren();
			  System.out.println("my child list is: " + childList);
			  
			  String filePath = currentPath +  File.separator + "state.txt";
			  String fileListPath= currentPath;
			  try {
				StateSplit file = new StateSplit(filePath, numParts);
				//FileSplit.splitFile(filePath, numParts, numReplica);
				numSpli = file.stateSpliter();
				System.out.println("Already partition the state!");
				file.makeReplica(numReplica);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} 
			  
			// root node get its following children nodes to save all replicas
			  NodeHandle[] nhlist = this.getChildren();
			  //nhlist.;
			  int numChildren = myScribe.numChildren(myTopic);
			  System.out.println(nhlist);
			  System.out.println("I have "+ numChildren + " children nodes.");
			  long startTime = System.currentTimeMillis();
			  //here anycast the replica of each part to the following nodes
			  try {
				  File directory = new File(fileListPath);
				  File[] listOfFiles = directory.listFiles();

				  for (File file : listOfFiles) {
					  
					String fileName = file.getName();
					String extension = FilenameUtils.getExtension(fileName);// saves the name of each part
					
					if (extension.contains("part")) {
						//Just in case too many msgs, some node may not receive
						Random random = new Random();
						int k = random.nextInt(200);
						try
						{
						    Thread.sleep(k); //wait for 5ms for waiting replicas
						}
						catch(InterruptedException ex)
						{
						    Thread.currentThread().interrupt();
						}
						
						
						String content = new Scanner(file).useDelimiter("\\Z").next();
						ScribeMessage myMessage = new ScribeMessage(this.endpoint.getId(), this.endpoint.getId(), content, extension, startTime);
						//NodeHandle nh= endpoint.getLocalNodeHandle();
						//myScribe.anycast(myTopic, myMessage, nh);
						myScribe.anycast(myTopic, myMessage);
						//here use route to send the part to any node in the myTopic. Note it may send to itself. This is dues to the bug of Pastry
						//endpoint.route(myTopic.getId(), myMessage, null);
						System.out.println(this.endpoint.getId()+ " :I dissemiate the different parts: " + file+ " to following nodes.");
					}					
				  }  
			
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  } else { //here after the second round, root will try to aggregate all replica to merge to original file
			  System.out.println("==========%%%%%%%%%%%%%%%%===========");
			  System.out.println("Okay! Let's aggregate the original file");
			  long startTime = System.currentTimeMillis();
			  /*
			   * Here we ask send to root(see details in RootSendSpeNodeMessage): star-structured recovery
			   * ask send to parent(see details in xxx): tree-structured recovery
			   * ask send to next part(see details in LinearCollectMessage): line-structured recovery
			   */
			  int  recoveryStructure = recoverySelection(appName, numParts, lengthPath, latencyRequire);
			  /*
			   * here for star-structured recovery: root finds all node who have the specific shard and ask them to send
			   */
			  if (recoveryStructure == 1){
				  for (Entry<Integer, List<Id>> entry : part_map.entrySet()) {
					     System.out.println("Key: " + entry.getKey() + ". Value: " + entry.getValue());
					     entry.getValue().get(0);
					     String partName = "part"+ entry.getKey();
					     RootSendSpeNodeMessage myMessage = new RootSendSpeNodeMessage(this.endpoint.getId(), entry.getKey(), this.endpoint.getId(), contentPart, partName, startTime);
						 endpoint.route(entry.getValue().get(0), myMessage, null);
					}
			  }			  
			  //*/
			  //now for line-structured recovery
			  if (recoveryStructure == 2){
				  Entry<Integer,List<Id>> entry = part_map.firstEntry();
				  if (entry.getKey() == 0) {
				     System.out.println("Key: " + entry.getKey() + ". Value: " + entry.getValue());
				     entry.getValue().get(0);
				     String partName = "part"+ entry.getKey();
				     LinearCollectMessage myMessage = new LinearCollectMessage(this.endpoint.getId(), this.endpoint.getId(), entry.getKey(), numSpli, null, startTime);
					 endpoint.route(entry.getValue().get(0), myMessage, null);
				  }
			  }
			  //now for tree-structured recovery
			  // here root checks its treemap and find which node for each part, then asks that node delivers its part to the parent
			  // note a replica for specific part only send once, so that there is no overlap in the parent
			  if (recoveryStructure == 3){
				  int replica_no = 0;
				  for (Entry<Integer, List<Id>> entry : part_map.entrySet()) {
					  //if (entry.getKey() == replica_no) {
					  	Random random = new Random();
						int k = random.nextInt(50);
						try
						{
						    Thread.sleep(k); //wait for 5ms for waiting replicas
						}
						catch(InterruptedException ex)
						{
						    Thread.currentThread().interrupt();
						}
					     System.out.println("Key: " + entry.getKey() + ". Value: " + entry.getValue());
					     entry.getValue().get(0);
					     //String partName = "part"+ entry.getKey();
					     //here ask this node sends this replica to parent
					     TreeMessage myMessage = new TreeMessage(this.endpoint.getId(), numSpli, entry.getKey(), null, "send the replica", startTime);
						 endpoint.route(entry.getValue().get(0), myMessage, null);
						 replica_no++;
				  }
			  }
		  }
		
	  }
	  if (message instanceof RootCollectMessage){
			//here RootCollectMessage means this node receives the replica from the root node
		    System.out.println("==================###### Root ######================================");
			System.out.println(endpoint.getId()+" : receive the part from node " +((RootCollectMessage) message).from); 
			
			String newPath= currentPath + File.separator + "R_" + endpoint.getId().toString().replaceAll("[^a-zA-Z0-9]", "");			
	  		
	  		File directory = new File(String.valueOf(newPath));
	  		if(!directory.exists()){
	  			new File(newPath).mkdirs();
	  		}
	  		FileWriter fw;
	  		String fileName = newPath + File.separator + "replica_" + ((RootCollectMessage) message).partName;
	  		
	  		String rootTimeFile = newPath + File.separator + "merge_results"+ File.separator + "rootAggregationTime.txt";
			String rootTimeDir = newPath + File.separator + "merge_results";

	  		File directory2 = new File(String.valueOf(rootTimeDir));
	  		if(!directory2.exists()){
	  			new File(rootTimeDir).mkdirs();
	  		}

			try {
				fw = new FileWriter(fileName, false);
				BufferedWriter bw = new BufferedWriter(fw);
			    PrintWriter out = new PrintWriter(bw);
			    out.println(((RootCollectMessage) message).content);				
			    out.close();
			    
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			long endTime = System.currentTimeMillis();
			/*
			 * Here is only used for centralized collection
			 * Need to comment when using tree-based recovery
			 */
			///*
			try
			{
			    Thread.sleep(5); //wait for 5ms for waiting all replicas
			}
			catch(InterruptedException ex)
			{
			    Thread.currentThread().interrupt();
			}
			this.rootCollectedNum ++;
			if (rootCollectedNum == (numSpli+1)) {
				//here aggregate all parts
				try {
					StateMerge file = new StateMerge(newPath, numSpli);
					System.out.println(endpoint.getId()+ newPath);
					file.stateMerger();
					System.out.println(endpoint.getId()+ " :Already merge the file!");
					//here root node calculates the aggregation latency for the state and save to a file
					FileWriter fw1;
					fw1 = new FileWriter(rootTimeFile, true);
					BufferedWriter bw = new BufferedWriter(fw1);
				    PrintWriter out = new PrintWriter(bw);
					 
				    out.println("Topic: "+ this.topic + ", " + this.endpoint.getId()+ 
				    		" : the aggregation totally takes " + (endTime - ((RootCollectMessage) message).time ));
					
				    out.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				this.rootCollectedNum = 0;
			}			
			//*/
			// only used for linear and tree recovery
			// keep this part to let root node calculates the aggregation latency for the state and save to a file
			/*
			FileWriter fw1;
			try {
				fw1 = new FileWriter(rootTimeFile, true);
				BufferedWriter bw = new BufferedWriter(fw1);
			    PrintWriter out = new PrintWriter(bw);
				 
			    out.println("Topic: "+ this.topic + ", " + this.endpoint.getId()+ 
			    		" : the aggregation totally takes " + (endTime - ((RootCollectMessage) message).time ));
				
			    out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			*/
			
		  }
	  if (message instanceof RootSendSpeNodeMessage){
		  
		//here RootSendSpeNodeMessage is for the node who receives the note from root and follow the note
		//to send the specific part to the root or parent
	    System.out.println("===================&&&&&&&&&&&==============================");
		System.out.println(endpoint.getId()+" : receive the aggregation note from root " +((RootSendSpeNodeMessage) message).from); 
		
		String newPath= currentPath + File.separator + endpoint.getId().toString().replaceAll("[^a-zA-Z0-9]", "");			
		File directory = new File(newPath);
		File[] listOfFiles = directory.listFiles();

		  for (File file : listOfFiles) { 
			  String fileName = file.getName();
			  String[] parts = fileName.split("_");
			  			  
			  if(parts.length > 1) { // means it has the "part" info, need to be merged
				  int firstPart = Integer.parseInt(parts[1])/10;// get the xx value of partxx
				  
				  if (firstPart == ((RootSendSpeNodeMessage) message).key) {
					  //now it finds the part that it should send to root or parent node
					  String content;
					try {
						content = new Scanner(file).useDelimiter("\\Z").next();
						String extension = parts[1]; // here is the name of partxx
 						/*
 						 * Here is for selecting different structure for recovery
 						 * -------------------------------------------------------------------------------
 						 * send to root: centralized recovery
 						 */	
						
						RootCollectMessage myMessage = new RootCollectMessage(this.endpoint.getId(), content, extension, ((RootSendSpeNodeMessage) message).time);
 						endpoint.route(((RootSendSpeNodeMessage) message).from, myMessage, null);
 						System.out.println(endpoint.getId() + " :Now I send the " + ((RootSendSpeNodeMessage) message).key + " part to root!");
 											 						
 						break; //here is use to send only once for specific part if current node has multiple replicas
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}						  
				  }
			  }
  				
		  }
			
		
			
	  }
	  if (message instanceof ParentCollectMessage){
		  	// this is used for the tree-aggregation partition, not the centralized or linear aggregation
		  	//here for parent node receive the part of file from its children, so it can aggregate parts 
		  	String newPath= currentPath + File.separator + "P_" + endpoint.getId().toString().replaceAll("[^a-zA-Z0-9]", "");			
	  		
	  		File directory = new File(String.valueOf(newPath));
	  		if(!directory.exists()){
	  			new File(newPath).mkdirs();
	  		}
	  		FileWriter fw;
	  		String fileName = newPath + File.separator + "replica_" + ((RootCollectMessage) message).partName;
			try {
				fw = new FileWriter(fileName, false);
				BufferedWriter bw = new BufferedWriter(fw);
			    PrintWriter out = new PrintWriter(bw);
			    out.println(((RootCollectMessage) message).content);				
			    out.close();
			    
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
			// now parent node merge the parts it has, then send to its parent, until to the root
			
			
	  }
	  /*
	   * This is only for tree-based collection of parts
	   */
	  if (message instanceof TreeMessage) {
		  //here is for child node received msg from root, then send specific replica to its parents
		  // used for tree-based recovery
		  if ( ((TreeMessage) message).mergeName == null){ // leaf nodes or parent nodes that have parts
			  	String newPath = currentPath + File.separator + endpoint.getId().toString().replaceAll("[^a-zA-Z0-9]", "");
				//for the first node who has the first part, just send to next
				File directory = new File(newPath);
				File[] listOfFiles = directory.listFiles();
				for (File file : listOfFiles) { 
					String fileName = file.getName();
					String[] parts = fileName.split("_");
					if(parts.length > 1) { // means it has the "part"
						  int firstPart = Integer.parseInt(parts[1])/10;// get the xx value of partxx
						  
						  if (firstPart == ((TreeMessage) message).key ) { //find the part that should send to its parent
							  try {
								String content_new = new Scanner(file).useDelimiter("\\Z").next();
								String partName = parts[1];
								TreeMessage myMessage2 = new TreeMessage(((TreeMessage) message).from, ((TreeMessage) message).numPart, 1, partName, content_new, ((TreeMessage) message).time);
								NodeHandle nh= this.getParent();
								
								if (nh == null) {//here it is the root of the tree, may not be the root of this topic, so it moves the replica to received path
									System.out.println(endpoint.getId()+ " I have no parent, so I just move my file: "+ file.getName());
									String move_newPath= newPath + File.separator + "received";					  		
							  		File move_directory = new File(String.valueOf(move_newPath));
							  		if(!move_directory.exists()){
							  			new File(move_newPath).mkdirs();
							  		}
									//file.renameTo(new File(newPath +"\\received\\" + file.getName()));
									
									InputStream inStream = null;
									OutputStream outStream = null;
									try{							    		
							    	    File bfile =new File(newPath + File.separator + "received" + File.separator + file.getName());							    		
							    	    inStream = new FileInputStream(file);
							    	    outStream = new FileOutputStream(bfile);							        	
							    	    byte[] buffer = new byte[1024];							    		
							    	    int length;
							    	    //copy the file content in bytes 
							    	    while ((length = inStream.read(buffer)) > 0){							    	  
							    	    	outStream.write(buffer, 0, length);							    	 
							    	    }							    	 
							    	    inStream.close();
							    	    outStream.close();							    	    
							    	    //delete the original file
							    	    //afile.delete();							    	    
							    	    System.out.println("File is copied successful!");
							    	    this.countNum++;
							    	}catch(IOException e){
							    	    e.printStackTrace();
							    	}
							    	break;
								}								
								if (nh != null) { //it has parent nodes
									Random random = new Random();
									int k = random.nextInt(20);
									try
									{
									    Thread.sleep(k); //wait for 5ms for waiting replicas
									}
									catch(InterruptedException ex)
									{
									    Thread.currentThread().interrupt();
									}
									
							    	endpoint.route(nh.getId(), myMessage2, null);
							    	System.out.println(endpoint.getId()+ " sends the "+ firstPart +" replica to the my parent: " + nh.getId());
							    	this.countNum = 0;
							    	break; //use to only send once
								}

							} catch (FileNotFoundException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						  }
					}
				}		  
		  }else { //here is for parent node: three parts: (1) save received replica from its child; 
			  	//(2) send all parts to its parent/root
			  	
			  	//(1) save received replica from its child
			  	String newPath= currentPath + File.separator + endpoint.getId().toString().replaceAll("[^a-zA-Z0-9]", "") + File.separator + "received";					  		
		  		File directory = new File(String.valueOf(newPath));
		  		if(!directory.exists()){
		  			new File(newPath).mkdirs();
		  		}
				//here send the number of merged parts to its parent or root
				File[] listOfFiles = directory.listFiles();
				NodeHandle nh= this.getParent();
				if (nh != null) {//it has parent
					this.countNum = this.countNum + ((TreeMessage) message).key + listOfFiles.length;
					String merge_content = "ok, let send number to parent" + this.countNum;
					TreeMessage myMessage2 = new TreeMessage(((TreeMessage) message).from, ((TreeMessage) message).numPart, 
							this.countNum, "merge_value", merge_content, ((TreeMessage) message).time);
					endpoint.route(nh.getId(), myMessage2, null);
			    	System.out.println(endpoint.getId()+ " sends the merged value "+ this.countNum +" to the my parent: " + nh.getId());
			    	this.countNum = 0;
			    	for (File file : listOfFiles) {
						file.delete();
					}
				}else{//root of the topic
					this.countNum = this.countNum + ((TreeMessage) message).key + listOfFiles.length;
					System.out.println("I am root and receive total " +this.countNum+ " parts");
					if (this.countNum == part_map.size()){
						//now it already received all parts
						RootCollectMessage myMessage3 = new RootCollectMessage(this.endpoint.getId(), "receive all", "end", ((TreeMessage) message).time);
				    	endpoint.route(((TreeMessage) message).from, myMessage3, null);
				    	System.out.println(endpoint.getId()+ " merge all parts, now send to "
					    	 		+ " the root node: " + ((TreeMessage) message).from);	
						this.countNum = 0;
						for (File file : listOfFiles) {
							file.delete();
						}
					}else{
						this.countNum = this.countNum - listOfFiles.length;
					}
				}				
				/*
				// here send the parts to its parents, if has no parent then check if gather all parts
				File[] listOfFiles = directory.listFiles();
				NodeHandle nh= this.getParent();
				if (nh != null) {
					//(2) send the parts it has to its parent
					for (File file : listOfFiles) { 
						
						Random random = new Random();
						int k = random.nextInt(30);
						try
						{
						    Thread.sleep(k); //wait for 5ms for waiting replicas
						}
						catch(InterruptedException ex)
						{
						    Thread.currentThread().interrupt();
						}
						
						String mergeName = file.getName();
						String[] parts = mergeName.split("_");						
						try {
							String merge_content = new Scanner(file).useDelimiter("\\Z").next();
							TreeMessage myMessage2 = new TreeMessage(((TreeMessage) message).from, ((TreeMessage) message).numPart, 
									Integer.parseInt(parts[1]), parts[1], merge_content, ((TreeMessage) message).time);
							endpoint.route(nh.getId(), myMessage2, null);
					    	System.out.println(endpoint.getId()+ " sends the merged "+ Integer.parseInt(parts[1]) +" replica to the my parent: " + nh.getId());
					    	file.delete(); //already send the merged part, so delete it
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}
					//(2) send info to its root	of topic
				}else{
					if (listOfFiles.length == part_map.size() ){  //(((TreeMessage) message).numPart +1)
						// it is root of the tree and receives all parts					
						//String final_message = new Scanner(file).useDelimiter("\\Z").next();
						for (File file : listOfFiles) { 
							file.delete();//now root of the tree can delete all
						}
						RootCollectMessage myMessage3 = new RootCollectMessage(((TreeMessage) message).from, "receive all", "end", ((TreeMessage) message).time);
				    	endpoint.route(((TreeMessage) message).from, myMessage3, null);
				    	System.out.println(endpoint.getId()+ " merge all parts, now send to "
					    	 		+ " the root node: " + ((TreeMessage) message).from);				
				    	
					}
					
				}
				*/
		  }					
	}			
	  /*
	   * TreeMessage is used for tree-parallel recovery
	   * This is one unfinished version, need to be modified later
	   */
	  /*
	  if (message instanceof TreeMessage) {
		  //here is for child node received msg from root, then send specific replica to its parents
		  // used for tree-based recovery
		  if ( ((TreeMessage) message).mergeName == null){ // leaf nodes or parent nodes that have parts
			  	String newPath = currentPath + File.separator + endpoint.getId().toString().replaceAll("[^a-zA-Z0-9]", "");
				//for the first node who has the first part, just send to next
				File directory = new File(newPath);
				File[] listOfFiles = directory.listFiles();
				for (File file : listOfFiles) { 
					String fileName = file.getName();
					String[] parts = fileName.split("_");
					if(parts.length > 1) { // means it has the "part"
						  int firstPart = Integer.parseInt(parts[1])/10;// get the xx value of partxx
						  
						  if (firstPart == ((TreeMessage) message).key ) { //find the part that should send to its parent
							  try {
								String content_new = new Scanner(file).useDelimiter("\\Z").next();
								String partName = parts[1];
								TreeMessage myMessage2 = new TreeMessage(((TreeMessage) message).from, ((TreeMessage) message).numPart, firstPart, partName, content_new, ((TreeMessage) message).time);
								NodeHandle nh= this.getParent();
								
								if (nh == null) {//here it is the root of the tree, may not be the root of this topic, so it moves the replica to received path
									System.out.println(endpoint.getId()+ " I have no parent, so I just move my file: "+ file.getName());
									String move_newPath= newPath + File.separator + "received";					  		
							  		File move_directory = new File(String.valueOf(move_newPath));
							  		if(!move_directory.exists()){
							  			new File(move_newPath).mkdirs();
							  		}
									//file.renameTo(new File(newPath +"\\received\\" + file.getName()));
									
									InputStream inStream = null;
									OutputStream outStream = null;
									try{							    		
							    	    File bfile =new File(newPath + File.separator + "received" + File.separator + file.getName());							    		
							    	    inStream = new FileInputStream(file);
							    	    outStream = new FileOutputStream(bfile);							        	
							    	    byte[] buffer = new byte[1024];							    		
							    	    int length;
							    	    //copy the file content in bytes 
							    	    while ((length = inStream.read(buffer)) > 0){							    	  
							    	    	outStream.write(buffer, 0, length);							    	 
							    	    }							    	 
							    	    inStream.close();
							    	    outStream.close();							    	    
							    	    //delete the original file
							    	    //afile.delete();							    	    
							    	    System.out.println("File is copied successful!");							    	    
							    	}catch(IOException e){
							    	    e.printStackTrace();
							    	}
							    	
								}								
								if (nh != null) { //it has parent nodes
									Random random = new Random();
									int k = random.nextInt(10);
									try
									{
									    Thread.sleep(k); //wait for 5ms for waiting replicas
									}
									catch(InterruptedException ex)
									{
									    Thread.currentThread().interrupt();
									}
									
							    	endpoint.route(nh.getId(), myMessage2, null);
							    	System.out.println(endpoint.getId()+ " sends the "+ firstPart +" replica to the my parent: " + nh.getId());
							    	break; //use to only send once
								}

							} catch (FileNotFoundException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						  }
					}
				}		  
		  }else { //here is for parent node: three parts: (1) save received replica from its child; 
			  	//(2) merge replica that it can merge; (3) send all parts to its parent/root
			  	
			  	//(1) save received replica from its child
			  	String newPath= currentPath + File.separator + endpoint.getId().toString().replaceAll("[^a-zA-Z0-9]", "") + File.separator + "received";					  		
		  		File directory = new File(String.valueOf(newPath));
		  		if(!directory.exists()){
		  			new File(newPath).mkdirs();
		  		}
		  		FileWriter fw;
		  		String fileName = newPath + File.separator + "receivedMerge_" + ((TreeMessage) message).mergeName ;
		  		//String fileName = newPath + File.separator +  ((TreeMessage) message).mergeName;
				try {
					fw = new FileWriter(fileName, false);
					BufferedWriter bw = new BufferedWriter(fw);
				    PrintWriter out = new PrintWriter(bw);
				    out.println(((TreeMessage) message).content);				
				    out.close();
				    
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
					
				//================================================================================
				//(2) merge replica that it can merge
				//here set a threshold for waiting a moment to wait for others children's replica
				
				try
				{
				    Thread.sleep(5); //wait for 5ms for waiting replicas
				}
				catch(InterruptedException ex)
				{
				    Thread.currentThread().interrupt();
				}
				try {
					TreeStateMerge file = new TreeStateMerge(newPath);
					System.out.println(endpoint.getId()+ " :Start to merge the replicas!");
					file.stateMerger();
					System.out.println(endpoint.getId()+ " :Already merge the replicas!");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	  
								
				//(3) send all parts to its parent/root
				
				File[] listOfFiles = directory.listFiles();
				for (File file : listOfFiles) { 
					String mergeName = file.getName();
					String[] parts = mergeName.split("_");
					if(parts.length == 5) { // means it has the "merge" info, the merged parts
						//int firstPart = Integer.parseInt(parts[1])/10;// get the xx value of partxx
						if ( Integer.parseInt(parts[2]) == this.part_map.firstKey() && Integer.parseInt(parts[4]) == this.part_map.lastKey()) {
							//here means it already merges all parts, can directly send to root							
							try {
								String final_message = new Scanner(file).useDelimiter("\\Z").next();
								RootCollectMessage myMessage3 = new RootCollectMessage(((TreeMessage) message).from, final_message, "end", ((TreeMessage) message).time);
						    	endpoint.route(((TreeMessage) message).from, myMessage3, null);
						    	System.out.println(endpoint.getId()+ " merge all parts, now send to "
							    	 		+ " the root node: " + ((TreeMessage) message).from);
						    	file.delete();//already send the merged part, so delete it
						    	break;
							} catch (FileNotFoundException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							};							
						}
						try {
							String merge_content = new Scanner(file).useDelimiter("\\Z").next();
							TreeMessage myMessage2 = new TreeMessage(((TreeMessage) message).from, 0, Integer.parseInt(parts[4]), mergeName, merge_content, ((TreeMessage) message).time);
							NodeHandle nh= this.getParent();
							if (nh != null) {
								endpoint.route(nh.getId(), myMessage2, null);
						    	System.out.println(endpoint.getId()+ " sends the merged "+ Integer.parseInt(parts[4]) +" replica to the my parent: " + nh.getId());
						    	file.delete(); //already send the merged part, so delete it
							}
					    } catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}else if (parts.length == 2) {// it sends the parts that have not been merged to parent
						//int firstPart = Integer.parseInt(parts[1])/10;// get the xx value of partxx
						try {
							String merge_content = new Scanner(file).useDelimiter("\\Z").next();
							TreeMessage myMessage3 = new TreeMessage(this.endpoint.getId(), 0, Integer.parseInt(parts[1]), mergeName, merge_content, ((TreeMessage) message).time);
							NodeHandle nh= this.getParent();
							if (nh != null) {
								endpoint.route(nh.getId(), myMessage3, null);
						    	System.out.println(endpoint.getId()+ " sends the merged "+ Integer.parseInt(parts[1]) +" replica to the my parent: " + nh.getId());
							}					    	
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				 
			  }
			 
		  }	
	  */
	  if (message instanceof LinearCollectMessage){		  	
			/*
			 * Here is for line recovery
			 * -------------------------------------------------------------------------------
			 * send to next: line recovery
			 */
		  	System.out.println("===================&&&&& Linear Collection &&&&&&==============================");
		  	String newPath = currentPath + File.separator + endpoint.getId().toString().replaceAll("[^a-zA-Z0-9]", "");
			//for the first node who has the first part, just send to next
			File directory = new File(newPath);
			File[] listOfFiles = directory.listFiles();
			int former_key = ((LinearCollectMessage) message).key;
			String content_old = ((LinearCollectMessage) message).content, content_new = null;
			for (File file : listOfFiles) { 
				String fileName = file.getName();
				String[] parts = fileName.split("_");
				if(parts.length > 1) { // means it has the "part" info, need to be merged
					  int firstPart = Integer.parseInt(parts[1])/10;// get the xx value of partxx
					  
					  if (firstPart == ((LinearCollectMessage) message).key && former_key == 0) { //former_key ==0 is used for merge the 0th part only once
						  try {
							content_new = new Scanner(file).useDelimiter("\\Z").next();
							content_old = content_old + content_new;
							//file.delete();
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						  former_key = firstPart;
					  }else if ((firstPart-1) == former_key ) {
						  try {// if part0, part1, and part2 here, merge them to one
							content_new = new Scanner(file).useDelimiter("\\Z").next();
							content_old = content_old + content_new;
							former_key = firstPart;
							System.out.println("=HeyHeyHey: now I have the part " + former_key);
							//file.delete();
							//break;
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					  }
				}
			}
			 for (Entry<Integer, List<Id>> entry : part_map.entrySet()) {
			     
			     if (entry.getKey() == (former_key + 1) ) { //find the node for part4, if part 1 2 3 have been merged here.
			    	 LinearCollectMessage myMessage2 = new LinearCollectMessage(this.endpoint.getId(), ((LinearCollectMessage) message).root, former_key + 1, ((LinearCollectMessage) message).numSpli, content_old, ((LinearCollectMessage) message).time);
			    	 endpoint.route(entry.getValue().get(0), myMessage2, null);
			    	 System.out.println(endpoint.getId()+ " receive the part from "+ ((LinearCollectMessage) message).from + " , now send merged part to "
			    	 		+ "next Key: " + entry.getKey() + " to the target node: " + entry.getValue().get(0));
			    	 break;
			     }else if (former_key == ((LinearCollectMessage) message).numSpli){//here mean already merge all parts, now send to root
			    	 RootCollectMessage myMessage3 = new RootCollectMessage(this.endpoint.getId(), content_old, "end", ((LinearCollectMessage) message).time);
			    	 endpoint.route(((LinearCollectMessage) message).root, myMessage3, null);
			    	 System.out.println(endpoint.getId()+ " receive the part from "+ ((LinearCollectMessage) message).from + " , now send all part to "
				    	 		+ " the root node: " + ((LinearCollectMessage) message).root);
			    	 break;
			     }
			    
			}

	  }
	  if (message instanceof TimeMessage){
		  if (this.myScribe.isRoot(myTopic)){
			 long receiveTime = System.currentTimeMillis();
			 //countNum ++;
			 //nodeValue = nodeValue + ((TimeMessage) message).time;
			 //System.out.println("Receive the msg from my root or leaf from  " +((TimeMessage) message).from + " takes " + (receiveTime - ((TimeMessage) message).time )); 
			 System.out.println("At the "+ countNum + " round receivemsg from leaf from  " +((TimeMessage) message).from + " takes " + (receiveTime - ((TimeMessage) message).time ) ); 
			 
			 FileWriter fw;
			try {
				fw = new FileWriter("U:/Eclipse workplace/RootReceiveTime.txt", true);
				BufferedWriter bw = new BufferedWriter(fw);
			    PrintWriter out = new PrintWriter(bw);
				 
			    out.println("At the "+ countNum + " round receivemsg from leaf from  " +((TimeMessage) message).from + " takes " + (receiveTime - ((TimeMessage) message).time ) );
				
			    out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  }else{ // maybe the parent or leaf
			 int numChild = this.myScribe.numChildren(myTopic);
			 if (numChild != 0){ // parent nodes
					//long receiveChildTime = System.currentTimeMillis();
			    	sendtoParent(((TimeMessage) message).from, ((TimeMessage) message).time);
			 }
		  }	  
	  }
		  
		  
  }
  
/**
   * here the root node send message to all its children nodes
 * @throws IOException 
   */
  public void sendMultiCast() throws IOException {	  
	  //System.out.println("---------------------------------------------------------------------------------------------------------");
	  System.out.println("Now send the part of file you received to me");
	  String content = "Hi";
	  long anyTime = 12;
	  ScribeMessage myMessage = new ScribeMessage(endpoint.getId(), this.endpoint.getId(), content, "aggregation", anyTime);	  
	  //RootVectorMessage myMessage = new RootVectorMessage(endpoint.getLocalNodeHandle(), content, myTopic);	  
	  myScribe.publish(myTopic, myMessage);
  }
  /**
   * Deliver the nodeValue to parent in Scribe
   */
  @Override
public void deliver (Topic topic, ScribeContent content){
	  if (content instanceof ScribeMessage){
			 //here for each node to save the info about all nodes saves the parts, a tree map for all saved info
			 //System.out.println("========##################========####################====="); 
			 String partName = ((ScribeMessage) content).content; // here get the string of partxx
			 String[] parts = partName.split("_");
			 if(parts.length > 1) {
				 int part_No = Integer.parseInt(parts[1])/10;// get the xx value of partxx
				 List<Id> list_part = new ArrayList<Id> ();
				 list_part.add(((ScribeMessage) content).from); //here to save the new info of part 
				 
				 //now check if current node has saved info of current part in some node
				 List<Id> list_old = part_map.get(part_No);
				 //System.out.println(list_old);
				 if ( list_old != null ) {
					 list_part.addAll(list_old);  //merge the info of same part into one list
				 }
				 //System.out.println(list_part);
				 part_map.put(part_No, list_part);
			 }
		  }
	  if (content instanceof ParentCollectMessage){
		// this is used for the tree-aggregation partition, not the star or line aggregation
		//here for children node send its part to the parent 
		int numChild = this.myScribe.numChildren(myTopic);
		if (numChild == 0){ // leaf nodes that may have parts
			String newPath = currentPath + File.separator + endpoint.getId().toString().replaceAll("[^a-zA-Z0-9]", "");
			//for the first node who has the first part, just send to next
			File directory = new File(newPath);
			File[] listOfFiles = directory.listFiles();
			int send_key = 0;
			String content_old  = null;
			for (File file : listOfFiles) { 
				String fileName = file.getName();
				String[] parts = fileName.split("_");
				if(parts.length > 1) { // means it has the "part" info, need to be merged
					  int firstPart = Integer.parseInt(parts[1])/10;// get the xx value of partxx
					  
					  if (firstPart == send_key ) { //send_key is used for check if this part had been sent
						  try {
							content_old = new Scanner(file).useDelimiter("\\Z").next();
							send_key++;
							NodeHandle nh= this.getParent();
							ParentCollectMessage myMessage3 = new ParentCollectMessage(this.endpoint.getId(), firstPart, content_old);
					    	endpoint.route(nh.getId(), myMessage3, null);
					    	System.out.println(endpoint.getId()+ " : I send my part "+ firstPart + " to "
						    	 		+ " my parent: " + nh.getId());
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					  }
				}
			}
		}
		
		
	  }
  }
  /**
   * Sends the message to parent.
   */
  public void sendtoParent(Id from , long recordtime) {
	
    NodeHandle nh= this.getParent();
    if(nh != null){
    	this.sendMsgDirect(nh, from, recordtime);
    	System.out.println("Sned to my parent!");
    }
  }

  /**
   * send the message directly to targeted node
   * @param nh
   */
  public void sendMsgDirect(NodeHandle nh, Id from, long recordtime) {
	  //String sg = endpoint.getId()+"-"+ nodeValue;
	  //Vector<String> vc = this.vecData;
	  Message msg = new TimeMessage(from, recordtime);
	  //Message msg = new VectorMessage(endpoint.getId(), nh.getId(), vc, recordtime);
	  endpoint.route(nh.getId(), msg, nh);

  }
  /**
   * Here to store file name into a buffer by root node
   */
  public void storeFileName(){
	  for(int i=1; i<=10; i++){
		  if (fileName[i] == null) {
			  fileName[i]= "string of file name"; // here need to modify the string
		  }
	  }
  }
  /**
   * here to get parent node of current node
   * @return nodeHandle of parent nodes
   */
  public NodeHandle getParent() {
	  return ((ScribeImpl)myScribe).getParent(myTopic);
  }
  
  
  @Override
  public boolean anycast(Topic arg0, ScribeContent arg1) {
  	// TODO Auto-generated method stub
	 
  	boolean returnValue = myScribe.getEnvironment().getRandomSource().nextInt(3) == 0;
      if (endpoint != null) {
        System.out.println("MyScribeClient.anycast(" +myTopic+ ","+arg1+"): " + endpoint.getId() + " " + returnValue);
        
        if (returnValue == true) {
	      	this.receivedPart = 1;
	        //here use to publish its received info so that other nodes can get all records
	  		System.out.println("----------------------------------------------");
	  		long receivedReplicaTime = System.currentTimeMillis();
	  		this.countReplica++;
	  		System.out.println(this.endpoint.getId()+" : Hey, now please records my saved info: " + ((ScribeMessage) arg1).partName
	  				+ ", I already have " + countReplica);
	  		long saveTime = receivedReplicaTime - ((ScribeMessage) arg1).time;
	  		System.out.println(this.endpoint.getId()+" receive the state takes " + saveTime);
	  		//System.out.println(this.endpoint.getId()+" : the Part name is : " + ((ScribeMessage) arg1).partName);
	  		System.out.println("----------------------------------------------");
	  		// save the received part into its own folder
	  		String newPath= currentPath + File.separator + endpoint.getId().toString().replaceAll("[^a-zA-Z0-9]", "");
	  		String fileName = newPath + File.separator + ((ScribeMessage) arg1).partName;
	  		File directory = new File(String.valueOf(newPath));
	  		if(!directory.exists()){
	  			new File(newPath).mkdirs();
	  		}
	  		FileWriter fw;
			try {
				fw = new FileWriter(fileName, true);
				BufferedWriter bw = new BufferedWriter(fw);
			    PrintWriter out = new PrintWriter(bw);
			    out.println(((ScribeMessage) arg1).content);				
			    out.close();
			    
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	  		
	  		
	  		
	  		String content = ((ScribeMessage) arg1).partName;
	  		long anyTime = 12;
	  		ScribeMessage myMessage = new ScribeMessage(this.endpoint.getId(), this.endpoint.getId(), content, content, anyTime);
	  		//braodcast to all nodes in the same topic, so that all nodes have a copy of saved info
	  		myScribe.publish(myTopic, myMessage);
        }
      }
      return returnValue;

  }
 
 Integer recoverySelection (String appName, int num, int length, String latencyRequire){
	 //here is for selecting different structures, can be redefined by users or developer
	 if (num < 16*1024){ //can be changed due to different requirements
		 return 1; //here is for star-structured recovery
		 
	 }else if (length > 128){// here is the length of recovery path, can be redefined
		 return 2; //here is for line-structured recovery
	 }else if (latencyRequire == "No"){
		 // here is the requirement of runtime latency, can be changed by users	 
		 return 2;
	 }else {
		 return 3;//here is for tree-structured recovery
	 }
	 	 
 }  
  
  @Override
public void subscribeFailed(Topic topic){
	  System.out.println("TreeApplication.childFailed("+topic+")");
  }
  @Override
public void childAdded(Topic topic, NodeHandle child) {
  //    System.out.println("MyScribeClient.childAdded("+topic+","+child+")");
  }

  @Override
public void childRemoved(Topic topic, NodeHandle child) {
  //    System.out.println("MyScribeClient.childRemoved("+topic+","+child+")");
  }
  @Override
public void update(NodeHandle handle, boolean joined) {
    
  }
  
  class PublishContent implements Message{
	  @Override
	public int getPriority(){
		  return MAX_PRIORITY;
	  }
  }

@Override
public boolean forward(RouteMessage arg0) {
	// TODO Auto-generated method stub
	return true;
}

public boolean isRoot() {
	// TODO Auto-generated method stub
	return myScribe.isRoot(myTopic);
}
public NodeHandle[] getChildren() {
	// TODO Auto-generated method stub
	return myScribe.getChildren(myTopic);   
}




}
