package com.dic.distributedga.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class ApplicationMasterGA {

	private static final Log log = LogFactory.getLog(ApplicationMasterGA.class);
	
	private Configuration conf;

	public static void main(String[] args) {
		boolean result = false;
		ApplicationMasterGA appMaster = new ApplicationMasterGA();

		log.info("initialize application master");
		boolean doRun = appMaster.init(args);
		if (!doRun) {
			System.exit(0);
		}
		
		appMaster.run();
		result = appMaster.finish();
		
		
		if(result) {
			log.info("Application master succesfully completed. exiting");
			System.exit(0);
		}
		else {
			log.info("Application master failed. exiting");
			System.exit(1);
		}
	}
	
	public ApplicationMasterGA(){
		conf = new YarnConfiguration();
	}
	
	public boolean init(String[] args){
		return true;
	}
	public void run(){
		
	}
	public boolean finish(){
		return true;
	}

}
