package com.dic.distributedga.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;

public class Population implements Serializable, Comparable<Population>{

    ArrayList<Individual> individuals;
    
    /*
     * Constructors
     */
    // Create a population
    public Population(int populationSize, boolean initialise) {
        individuals = new ArrayList<Individual>(populationSize);
        // Initialise population
        if (initialise) {
            // Loop and create individuals
            for (int i = 0; i < populationSize; i++) {
                Individual newIndividual = new Individual();
                newIndividual.generateIndividual();
                saveIndividual(newIndividual);
            }
        }
    }

    public Population(ArrayList<Individual> individuals){
    	this.individuals = individuals;
    }
    
    /* Getters */
    public Individual getIndividual(int index) {
        return individuals.get(index);
    }

    public Individual getFittest() {
        Individual fittest = getIndividual(0);
        // Loop through individuals to find fittest
        for (int i = 0; i < size(); i++) {
            if (fittest.getFitness() <= getIndividual(i).getFitness()) {
                fittest = getIndividual(i);
            }
        }
        return fittest;
    }
    
    public int getWeakestIndex(){
    	Individual weakest = getIndividual(0);
    	int weakestIndex =0;
        // Loop through individuals to find fittest
        for (int i = 1; i < size(); i++) {
            if (getIndividual(weakestIndex).getFitness() >= getIndividual(i).getFitness()) {
            	weakestIndex = i;
            }
        }
        return weakestIndex;
    }

    /* Public methods */
    // Get population size
    public int size() {
        return individuals.size();
    }

    // Save individual
    public void saveIndividualAtIndex(int index, Individual indiv) {
        individuals.add(index, indiv);
    }
    public void saveIndividual(Individual indiv){
    	individuals.add(indiv);
    }
    
    /*return a subset of total population*/
    public Population getPopulationSubset(int startIndex, int endIndex){
    	int size = endIndex-startIndex+1;
    	if(size <= 0)
    		return null;
    	Population p = new Population(size, false);
    
    	for(int i = startIndex; i <= endIndex; i++)
    		p.saveIndividual( getIndividual(i));
    	return p;
    }
    
    public void removeWeakest(){
    	individuals.remove(getWeakestIndex());
    }
    public void integrateMigrants(Population immigrantsPop){
    	for(int i = 0; i < immigrantsPop.size(); i++){
    		removeWeakest();
    	}
    	
    	for(int i = 0; i < immigrantsPop.size(); i++){
    		saveIndividual(immigrantsPop.getIndividual(i));
    	}
    }

	public Population getFittestSubset(int migrantSize) {

		Population pop = new Population( (ArrayList<Individual>) individuals.clone());
		pop.sortIndividuals();
		
		Population migrantPop = new Population(migrantSize,false);
		for(int i = 0; i < migrantSize; i++){
			migrantPop.saveIndividual(pop.getIndividual(i));
		}
		
		return migrantPop;
	}
	
	public int totalFitness(){
		int fitness =0;
		for(Individual individual:individuals){
			fitness += individual.getFitness();
		}
		return fitness;
	}
	private void sortIndividuals(){
		Collections.sort(individuals, Individual.SORT_DECREASING_ORDER);
	}

	public int compareTo(Population other) {
		return totalFitness() - other.totalFitness();
	}

}