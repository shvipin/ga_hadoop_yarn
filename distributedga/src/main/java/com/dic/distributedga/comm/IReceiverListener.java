package com.dic.distributedga.comm;

public interface IReceiverListener {

	public void readyReceivedEvent(GAWorkContext gaWorkContext);
	public void terminationReceivedEvent(GAWorkContext gaWorkContext);
	public void initialPopReceivedEvent(GAWorkContext gaWorkContext);
	public void migrantPopReceivedEvent(GAWorkContext gaWorkContext);
	
}
