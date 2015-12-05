package com.dic.distributedga.comm;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

import com.dic.distributedga.Utils;

/**
 * This class will listen on port to listen for events Events can be New
 * individuals of population Termination
 *
 */
public class Receiver {
	ServerSocket listener;

	public Receiver(int portNo, int timeout) throws IOException {
		listener = new ServerSocket(portNo);
		listener.setSoTimeout(timeout);
	}
	public void closeReceiver() throws IOException{
		listener.close();
	}
	public Socket startListening() throws IOException {
		return listener.accept();
	}

}
