package pastry_replica;

import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.scribe.ScribeContent;


public class ScribeMessage implements ScribeContent, Message {
 
	Id from;
	Id root;
	
	String content;
	
	String partName;
	
	long time;
	
	public ScribeMessage(Id id, Id root, String content, String partName, long time){
		this.from = id;
		this.root = root;
		this.content = content;
		this.partName = partName;
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
