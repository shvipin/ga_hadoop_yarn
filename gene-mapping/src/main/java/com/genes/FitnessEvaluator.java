package com.genes;

public class FitnessEvaluator {
	private String optimalSolutionString = "11110000000000000111111110000000000000000";
	private byte[] optimalSolution;
	
	private static FitnessEvaluator __instance = new FitnessEvaluator();
	private FitnessEvaluator() {
		this.setSolution(optimalSolutionString);
	}
	
	public static FitnessEvaluator getInstance(){
		return __instance;
	}
		
	public double getFitness(BinaryChromosome chromosome){
		double fitness = 0;
		for(int i = 0 ;i < chromosome.size() && i < optimalSolution.length; i++){
			if(chromosome.getGene(i).getGene() == optimalSolution[i])
				fitness += 1;
		}
		return fitness;
	}
	
	public double getBestFitness(){
		return optimalSolution.length;
	}
	
	public int getSolutionLength(){
		return optimalSolution.length;
	}
	
	public void setSolution(String newSolution) {
		optimalSolution = new byte[newSolution.length()];
        for (int i = 0; i < newSolution.length(); i++) {
            String character = newSolution.substring(i, i + 1);
            if (character.contains("0") || character.contains("1")) {
            	optimalSolution[i] = Byte.parseByte(character);
            } else {
            	optimalSolution[i] = 0;
            }
        }
    }

	
}
