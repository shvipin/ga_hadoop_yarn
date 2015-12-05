package com.dic.distributedga.comm;

public interface IReceiverListener {

	public void readyReceivedEvent(String ipAddress, GAWorkContext gaWorkContext);
	public void terminationReceivedEvent(String ipAddress, GAWorkContext gaWorkContext);
	public void initialPopReceivedEvent(String ipAddress, GAWorkContext gaWorkContext);
	public void migrantPopReceivedEvent(String ipAddress, GAWorkContext gaWorkContext);
	
}
