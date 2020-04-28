package pastry_replica;


import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.scribe.ScribeContent;


public class TreeMessage implements ScribeContent, Message {
 
	Id from;
	
	int numPart;
	int key;
	String mergeName;
	String content;
	long time;
	
	public TreeMessage(Id id, Integer totalPart, Integer key, String mergeName, String content, long time){
		this.from = id;
		this.key = key;
		this.numPart = totalPart;
		this.mergeName = mergeName;
		this.content = content;
		this.time = time;
		
	}
	/*
	public String toString(){
		return "Scribe message"+ seq+ " from "+from;
	}
	*/


	@Override
	public int getPriority() {
		// TODO Auto-generated method stub
		return 0;
	}
}
