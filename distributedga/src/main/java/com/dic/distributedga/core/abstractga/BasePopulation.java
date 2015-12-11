package com.dic.distributedga.core.abstractga;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;

public abstract class BasePopulation implements Serializable {
	ArrayList<BaseChromosome> individuals;
	Class chromosomeClass;
	Class geneClass;
	int geneLength;

	public BasePopulation(Class<? extends BaseChromosome> chromosomeClass, Class<? extends BaseGene> geneClass,
			int populationSize, int geneLength, boolean initialise)
					throws InstantiationException, IllegalAccessException, IllegalArgumentException,
					InvocationTargetException, NoSuchMethodException, SecurityException {
		individuals = new ArrayList<BaseChromosome>(populationSize);
		this.chromosomeClass = chromosomeClass;
		this.geneClass = geneClass;
		this.geneLength = geneLength;
		if (initialise) {
			for (int i = 0; i < populationSize; i++) {
				
				Class[] argType = { Class.class, int.class};
				Object[] args = { geneClass, new Integer(geneLength)};
				Constructor constructor = chromosomeClass.getConstructor(argType);

				BaseChromosome individual = (BaseChromosome)constructor.newInstance(args);
						
				individual.generateChromosome();
				saveChromosome(individual);
			}
		}
	}
	public void saveChromosome(BaseChromosome chromosome) {
		individuals.add(chromosome);
	}

	public BaseChromosome getChromosome(int index) {
		return individuals.get(index);
	}

	public BaseChromosome getFittest() {
		BaseChromosome fittest = getChromosome(0);
		// Loop through individuals to find fittest
		for (int i = 0; i < size(); i++) {
			if (fittest.getFitness() <= getChromosome(i).getFitness()) {
				fittest = getChromosome(i);
			}
		}
		return fittest;
	}

	public int getWeakestIndex() {
		BaseChromosome weakest = getChromosome(0);
		int weakestIndex = 0;
		// Loop through individuals to find fittest
		for (int i = 1; i < size(); i++) {
			if (getChromosome(weakestIndex).getFitness() >= getChromosome(i).getFitness()) {
				weakestIndex = i;
			}
		}
		return weakestIndex;
	}

	public int size() {
		return individuals.size();
	}

	// Save individual
	public void saveChromosomeAtIndex(int index, BaseChromosome indiv) {
		individuals.add(index, indiv);
	}

	/* return a subset of total population */
	public abstract BasePopulation getPopulationSubset(int startIndex, int endIndex)
			throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException;

	public abstract BasePopulation getFittestSubset(int migrantSize)
			throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException, CloneNotSupportedException;

	public void removeWeakest() {
		individuals.remove(getWeakestIndex());
	}

	public void integrateMigrants(BasePopulation immigrantsPop) {
		for (int i = 0; i < immigrantsPop.size(); i++) {
			removeWeakest();
		}

		for (int i = 0; i < immigrantsPop.size(); i++) {
			saveChromosome(immigrantsPop.getChromosome(i));
		}
	}

	public double totalFitness() {
		double fitness = 0;
		for (BaseChromosome individual : individuals) {
			fitness += individual.getFitness();
		}
		return fitness;
	}

	public void sortChromosomes() {
		Collections.sort(individuals, BaseChromosome.SORT_DECREASING_ORDER);
	}

	public int compareTo(BasePopulation other) {
		return (int) (totalFitness() - other.totalFitness());
	}

}
