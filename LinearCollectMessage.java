package pastry_replica;

import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.scribe.ScribeContent;


public class LinearCollectMessage implements ScribeContent, Message {
 
	Id from;
	Id root;
	
	int numSpli;
	int key;
	String content;
	long time;
	
	public LinearCollectMessage(Id id, Id root, int key, int numSpli, String content, long time){
		this.from = id;
		this.root = root;
		
		this.numSpli = numSpli;
		this.key = key;
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
