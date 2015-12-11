package com.maxeval.maxeval;

public class FitnessEvaluator {
	// bin: "111111111111111111111111"; //16777215 in decimal 24 characters
	private int maxFitness = 16777215;
	private int maxLength = 24;
	private static FitnessEvaluator __instance = new FitnessEvaluator();
	private FitnessEvaluator() {
//		this.setSolution(optimalSolutionString);
	}
	
	public static FitnessEvaluator getInstance(){
		return __instance;
	}
		
	public double getFitness(BinaryChromosome chromosome){
		double fitness = 0;
		for(int i = 0 ;i < chromosome.size() ; i++){
			fitness += Math.pow(2, chromosome.size() - i - 1)*chromosome.getGene(i).getGene();				
		}
		return fitness;
	}
	
	public double getBestFitness(){
		return maxFitness;
	}
	public int getLength(){
		return maxLength;
	}
	
}
