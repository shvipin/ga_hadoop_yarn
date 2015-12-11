package com.genes;

import java.lang.reflect.InvocationTargetException;

import com.dic.distributedga.core.Individual;
import com.dic.distributedga.core.Population;
import com.dic.distributedga.core.abstractga.BaseChromosome;
import com.dic.distributedga.core.abstractga.BasePopulation;
import com.dic.distributedga.core.abstractga.GAOperators;

public class BinaryGeneOperators extends GAOperators{
	int tournamentSize = 5;
	boolean elitism = true; //default
	int popSize = 120;
	FitnessEvaluator fitnessEvaluator = FitnessEvaluator.getInstance();
	int geneLength = fitnessEvaluator.getSolutionLength();
	
	public BinaryGeneOperators() {
		
		// TODO Auto-generated constructor stub
	}

	@Override
	public BasePopulation evolvePopulation(BasePopulation pop) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		BinaryPopulation newPopulation = new BinaryPopulation(BinaryChromosome.class, BinaryGene.class, pop.size(), geneLength, false);
	
	    // Keep our best individual
	    if (elitism) {
	        newPopulation.saveChromosome(pop.getFittest());
	    }
	
	    // Crossover population
	    int elitismOffset;
	    if (elitism) {
	        elitismOffset = 1;
	    } else {
	        elitismOffset = 0;
	    }
	    // Loop over the population size and create new individuals with
	    // crossover
	    for (int i = elitismOffset; i < pop.size(); i++) {
	    	BinaryChromosome chromosome1 = null, chromosome2 = null;
	        tournamentSelection(pop, (BinaryChromosome)chromosome1);
	        tournamentSelection(pop, (BinaryChromosome)chromosome2);
	        BinaryChromosome childChromosome = null;
	        crossoverFunction(chromosome1, chromosome2, childChromosome);
	        newPopulation.saveChromosome(childChromosome);
	    }
	
	    // Mutate population
	    for (int i = elitismOffset; i < newPopulation.size(); i++) {
	        mutationFunction(newPopulation.getChromosome(i));
	    }
	
	    return newPopulation;
	}


	@Override
	public void tournamentSelection(BasePopulation pop, BaseChromosome fittest) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		 
		 // Create a tournament population
		 BinaryPopulation tournament = new BinaryPopulation(BinaryChromosome.class, BinaryGene.class,  tournamentSize, geneLength, false);
		 // For each place in the tournament get a random individual
		 for (int i = 0; i < tournamentSize; i++) {
			 int randomId = (int) (Math.random() * pop.size());
			 tournament.saveChromosome( pop.getChromosome(randomId));
		 }
		 // Get the fittest
		 fittest = (BinaryChromosome) tournament.getFittest();
	}

	@Override
	public BasePopulation initStartPopulation() throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		BinaryPopulation pop = new BinaryPopulation(BinaryChromosome.class, BinaryGene.class, popSize, geneLength, true);
		return pop;
	}

	@Override
	public boolean isConvergenceReached(BasePopulation pop) throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		if(pop.getFittest().getFitness() >= FitnessEvaluator.getInstance().getBestFitness())
			return true;
		else return false;
	}

}
