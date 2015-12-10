package com.dic.distributedga;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.dic.distributedga.yarn.Client;

public class JobClient {
	
	private static final Log log = LogFactory.getLog(JobClient.class);

	private GAConfig config;
	private Client amSubmissionClient;

	public JobClient(GAConfig config){
		this.config = config;
		amSubmissionClient = new Client(config);
	}

	public void runJob(){
		if(!isConfigCorrect(config))
			throw new IllegalArgumentException("Config is not properly created");
		boolean result =false;
		try {

			amSubmissionClient.init();
			result = amSubmissionClient.run();

		} catch (IllegalArgumentException e) {
			e.getLocalizedMessage();
			System.exit(2);
		} catch (YarnException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (result) {
			log.info("Application completed successfully");
			System.exit(0);
		}

		log.info("Applicatoin failed to complete successfully");
		System.exit(1);
		
	}
	private static boolean isConfigCorrect(GAConfig config){
		return true;
	}

}
