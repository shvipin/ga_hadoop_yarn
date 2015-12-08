package com.dic.distributedga;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class Utils {

	public static final int MSG_TERMINATE        = 0x1;
	public static final int MSG_INIT_POP         = 0x2;
	public static final int MSG_MIGRATION_POP    = 0x4;
    public static final int MSG_READY            = 0x8;
    
    public static final int EVOLUTION_BATCH_SIZE = 20;
    public static final int MIGRANT_SIZE         = 5;
    
    public static final String USER_GA_JAR_HDFS_LOC = "USER_GA_JAR_HDFS_LOC";
    public static final String USER_GA_JAR_HDFS_TIMESTAMP = "USER_GA_JAR_HDFS_TIMESTAMP";
    public static final String USER_GA_JAR_HDFS_LEN = "USER_GA_JAR_HDFS_LEN";

    public static Socket createSocket(String ipAddress,int portNo) throws UnknownHostException, IOException{
    	Socket socket = new Socket(ipAddress, portNo);
    	socket.setTcpNoDelay(true);
    	return socket;
    }
	
}
