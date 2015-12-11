package com.genes;

public class FitnessEvaluator {

	private String optimalSolutionString = "111100101011011101010101010100000101010101010101010101010100110111100001111111000111000000111100101011011101010101010100000101010101010101010101010100110111100001111111000111000000111100101011011101010101010100000101010101010101010101010100110111100001111111000111000000111100101011011101010101010100000101010101010101010101010100110111100001111111000111000000111100101011011101"
			+ "000000000000000000000000000000000000000000000000000000000000000000000000000000000000001111111111111111111111111111111111111111111111111111111111111111111111111111000000000000000000000000000"
			+ "11111111111111111111111100000000000000000000000000000000111111111111110101010101010100101001010100101001010010101001010010100101010010100101010100101010101010100000101010101010101010101010100110111100001111111000111000000";
	private byte[] optimalSolution;

	private static FitnessEvaluator __instance = new FitnessEvaluator();

	private FitnessEvaluator() {
		this.setSolution(optimalSolutionString);
	}

	public static FitnessEvaluator getInstance() {
		return __instance;
	}

	public double getFitness(BinaryChromosome chromosome) {
		double fitness = 0;
		for (int i = 0; i < chromosome.size() && i < optimalSolution.length; i++) {
			if (chromosome.getGene(i).getGene() == optimalSolution[i])
				fitness += 1;
		}
		return fitness;
	}

	public double getBestFitness() {
		return optimalSolution.length;
	}

	public int getSolutionLength() {
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
