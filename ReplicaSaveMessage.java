package pastry_replica;


import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.scribe.ScribeContent;


public class ReplicaSaveMessage implements ScribeContent, Message {
 
	Id from;
	
	String content;
	
	public ReplicaSaveMessage(Id id, String content){
		this.from = id;
		this.content = content;
		
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