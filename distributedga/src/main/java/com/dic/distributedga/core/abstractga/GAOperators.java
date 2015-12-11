package com.dic.distributedga.core.abstractga;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

import com.dic.distributedga.GAConfig;

public abstract class GAOperators{
	private double selectionBias = 0.5; // default
	private double mutationRate = 0.015; // deafult

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

	
	
	// Mutate an individual
	public void mutationFunction(BaseChromosome indiv) {
		// Loop through genes
		for (int i = 0; i < indiv.size(); i++) {
			if (Math.random() <= getMutationRate()) {
				// set gene to random value
				indiv.getGene(i).setGene(indiv.getGene(i).getRandomGene());
			}
		}
	}
	
	public abstract BaseChromosome crossoverFunction(BaseChromosome bc1, BaseChromosome bc2 ) throws InstantiationException, IllegalAccessException;
	public abstract BasePopulation evolvePopulation(BasePopulation pop) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException;
	public abstract BaseChromosome tournamentSelection(BasePopulation pop) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException;
	public abstract BasePopulation initStartPopulation() throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException;
	public abstract boolean isConvergenceReached(BasePopulation pop) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException;
	
}
