package com.dic.distributedga.core.abstractga;

import com.dic.distributedga.GAConfig;

public abstract class GAOperators {
	private double selectionBias = 0.5; // default
	private double mutationRate = 0.015; // deafult

	public GAOperators(double selectionBias, double mutationRate) {
		this.selectionBias = selectionBias;
		this.mutationRate = mutationRate;
	}

	public double getSelectionBias() {
		return selectionBias;
	}

	public void setSelectionBias(double selectionBias) {
		this.selectionBias = selectionBias;
	}

	public double getMutationRate() {
		return mutationRate;
	}

	public void setMutationRate(double mutationRate) {
		this.mutationRate = mutationRate;
	}

	public void crossoverFunction(BaseChromosome bc1, BaseChromosome bc2, BaseChromosome bcChild) {
		// Loop through genes
		for (int i = 0; i < bc1.size(); i++) {
			// Crossover
			if (Math.random() <= getSelectionBias()) {
				bcChild.setGene(i, bc1.getGene(i));
			} else {
				bcChild.setGene(i, bc2.getGene(i));
			}
		}

	}

	// Mutate an individual
	public void MutationFunction(BaseChromosome indiv) {
		// Loop through genes
		for (int i = 0; i < indiv.size(); i++) {
			if (Math.random() <= getMutationRate()) {
				// set gene to random value
				indiv.getGene(i).setGene(indiv.getGene(i).getRandomGene());
			}
		}
	}

	public abstract BasePopulation evolvePopulation(BasePopulation pop);

	// Select individuals for crossover
	public abstract void tournamentSelection(BasePopulation pop, BaseChromosome fittest);

	// {
	// // Create a tournament population
	// PopulationC tournament = new PopulationC(tournamentSize, false);
	// // For each place in the tournament get a random individual
	// for (int i = 0; i < tournamentSize; i++) {
	// int randomId = (int) (Math.random() * pop.size());
	// tournament.saveChromosome( pop.getChromosome(randomId));
	// }
	// // Get the fittest
	// Chromosome fittest = tournament.getFittest();
	// return fittest;
	// }
}
