package pastry_replica;

import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;

public class RootSendSpeNodeMessage implements Message {
	Id from;
	Id target;
	
	int key;
	String content;
	String partName;
	long time;
	
	public RootSendSpeNodeMessage(Id id, int key, Id target, String content, String partName, long time){
		this.from = id;
		this.key = key;
		this.target = target;
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
