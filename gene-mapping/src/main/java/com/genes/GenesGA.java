package com.genes;

import com.dic.distributedga.GAConfig;
import com.dic.distributedga.JobClient;

public class GenesGA {

	public static void main(String[] args){
		GAConfig config = new GAConfig();
		
		config.setAppName("GeneMappingGA");
		config.setMasterMemory(1024);
		config.setMasterVCores(1);
		config.setContainersCount(3);
		config.setContainersMemory(1024);
		config.setContainersVCores(1);
		config.setJarPath(args[0]);
		config.setPortNo(6000);
		
		config.setDerPopulation(BinaryPopulation.class);
		config.setDerChromosome(BinaryChromosome.class);
		config.setDerGene(BinaryGene.class);
		config.setDerGAOperators(BinaryGeneOperators.class);

		
		
		
		//set extend required classes abstract function and add those 
		//classes in GAConfig.
		
		JobClient jobClient = new JobClient(config);
		jobClient.runJob();
	}
}