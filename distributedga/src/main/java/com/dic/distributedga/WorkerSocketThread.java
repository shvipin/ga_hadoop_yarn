package com.dic.distributedga;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

import com.dic.distributedga.comm.GAWorkContext;
import com.dic.distributedga.comm.IReceiverListener;

public class WorkerSocketThread extends Thread {

	Socket socket;
	private boolean terminate = false;
	private IReceiverListener listener;
	private final Object lock = new Object();

	public WorkerSocketThread(Socket socket, IReceiverListener listener) {
		this.socket = socket;
		this.listener = listener;
	}

	public void setTerminate(){
		synchronized(lock){
			terminate = true;
		}
	}
	
	public boolean isThreadDead(){
		return isAlive();
	}
	
	@Override
	public void run() {
		try {
			ObjectInputStream objInStream = new ObjectInputStream(socket.getInputStream());

			while (true) {
				
				synchronized(lock){
					if(terminate == true){
						break;
					}
				}
				
				GAWorkContext workContext = (GAWorkContext) objInStream.readObject();
				System.out.println("received from client ");
				switch (workContext.getFlag()) {
				case Utils.MSG_TERMINATE:
					
					if(listener != null)
						listener.terminationReceivedEvent(workContext);
					break;
				
				case Utils.MSG_READY:
					
					if(listener != null)
						listener.readyReceivedEvent(workContext);
					break;

				case Utils.MSG_INIT_POP:
					if(listener!= null)
						listener.initialPopReceivedEvent(workContext);
					break;
					
				case Utils.MSG_MIGRATION_POP:
					if(listener != null)
						listener.migrantPopReceivedEvent(workContext);
					break;
					
				default:

				}
			}
			objInStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
