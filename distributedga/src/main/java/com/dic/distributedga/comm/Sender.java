package com.dic.distributedga.comm;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.UnknownHostException;

import com.dic.distributedga.Utils;
import com.dic.distributedga.core.Population;

/**
 * This class will send messages to other nodes. Messages can be sending good
 * individuals in population Sending Best result
 */
public class Sender {

	
	private void sendGAWorkContext(GAWorkContext gaWorkContext,ObjectOutputStream os) throws UnknownHostException, IOException {
		os.writeObject(gaWorkContext);
		os.flush();
	}

	public void sendReady(ObjectOutputStream os) throws UnknownHostException, IOException{
		GAWorkContext gaWorkContext = new GAWorkContext();
		gaWorkContext.setFlag(Utils.MSG_READY);
		
		sendGAWorkContext(gaWorkContext,os);
	}
	public void sendInitPopulation(Population population, String sol, ObjectOutputStream os) throws UnknownHostException, IOException  {

		GAWorkContext obj = new GAWorkContext();
		obj.setPopulation(population);
		obj.setSolution(sol);
		obj.setFlag(Utils.MSG_INIT_POP);
		
		sendGAWorkContext(obj,os);
	}

	public void migratePopulation(Population population, ObjectOutputStream os) throws UnknownHostException, IOException {
		GAWorkContext obj = new GAWorkContext();
		obj.setPopulation(population);
		obj.setFlag(Utils.MSG_MIGRATION_POP);
		
		sendGAWorkContext(obj,os);
	}

	public void sendTerminateMsg(Population population, ObjectOutputStream os) throws UnknownHostException, IOException {
		GAWorkContext obj = new GAWorkContext();
		obj.setPopulation(population);
		obj.setFlag(Utils.MSG_TERMINATE);
		
		sendGAWorkContext(obj,os);
	}
	
	
}
