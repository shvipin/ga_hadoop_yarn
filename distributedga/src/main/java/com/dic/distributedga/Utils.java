package com.dic.distributedga;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

public class Utils {


	public static final int MSG_TERMINATE = 0x1;
	public static final int MSG_INIT_POP = 0x2;
	public static final int MSG_MIGRATION_POP = 0x4;
	public static final int MSG_READY = 0x8;

	public static final int EVOLUTION_BATCH_SIZE = 20;
	public static final int MIGRANT_SIZE = 5;

	public static final String USER_GA_JAR_HDFS_LOC = "USER_GA_JAR_HDFS_LOC";
	public static final String USER_GA_JAR_HDFS_TIMESTAMP = "USER_GA_JAR_HDFS_TIMESTAMP";
	public static final String USER_GA_JAR_HDFS_LEN = "USER_GA_JAR_HDFS_LEN";
	public static final	String GA_PORT_STRING = "6000";

	public static Socket createSocket(String ipAddress, int portNo) throws UnknownHostException, IOException {
		Socket socket = new Socket(ipAddress, portNo);
		socket.setTcpNoDelay(true);
		return socket;
	}

	public static void updateLog4jConfiguration(Class<?> targetClass, String log4jpath) throws IOException {

		Properties customProperties = new Properties();
		FileInputStream fs = null;
		InputStream is = null;
		try {
			fs = new FileInputStream(log4jpath);
			is = targetClass.getResourceAsStream("/log4j.properties");
			customProperties.load(fs);

			Properties originalProperties = new Properties();
			originalProperties.load(is);

			for (Entry<Object, Object> entry : customProperties.entrySet()) {
				originalProperties.setProperty(entry.getKey().toString(), entry.getValue().toString());
			}
			LogManager.resetConfiguration();
			PropertyConfigurator.configure(originalProperties);
		} finally {
			IOUtils.closeQuietly(is);
			IOUtils.closeQuietly(fs);
		}
	}

}
