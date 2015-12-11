package com.dic.distributedga.core.abstractga;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;



public abstract class BasePopulation {
	ArrayList<BaseChromosome> individuals;
	Class chromosomeClass;
	Class geneClass;
	int geneLength;
	public BasePopulation(Class<? extends BaseChromosome> chromosomeClass, 
			Class<? extends BaseGene> geneClass, 
			int populationSize, 
			int geneLength, 
			boolean initialise) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		
		individuals = new ArrayList<BaseChromosome>(populationSize);
		this.chromosomeClass = chromosomeClass;
		this.geneClass = geneClass;
		this.geneLength = geneLength;
		if(initialise){
			for(int i = 0; i< populationSize;i++){
				BaseChromosome individual = chromosomeClass.getDeclaredConstructor(geneClass, int.class).newInstance(geneClass, geneLength);
				individual.generateChromosome();
				saveChromosome(individual);
			}
		}
	}
	
	public void saveChromosome(BaseChromosome chromosome){
		individuals.add(chromosome);
	}
	
	public BasePopulation(ArrayList<BaseChromosome> o_chromosomes){
		 this.individuals = o_chromosomes;
	 }
	 
	 public BaseChromosome getChromosome(int index) {
		 return individuals.get(index);
	 }
	 
	 public BaseChromosome getFittest(){		 
		 BaseChromosome fittest = getChromosome(0);
	        // Loop through individuals to find fittest
	        for (int i = 0; i < size(); i++) {
	            if (fittest.getFitness() <= getChromosome(i).getFitness()) {
	                fittest = getChromosome(i);
	            }
	        }
	        return fittest;
	 }
	 
	 public int getWeakestIndex(){
		 BaseChromosome weakest = getChromosome(0);
	    	int weakestIndex =0;
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
	
	/*return a subset of total population*/
	public abstract BasePopulation getPopulationSubset(int startIndex, int endIndex);
//	{
//		int size = endIndex-startIndex+1;
//		if(size <= 0)
//			return null;
//		for(int i = startIndex; i <= endIndex; i++)
//			p.saveChromosome( getChromosome(i));
//		return p;
//	}
	    
	public abstract BasePopulation getFittestSubset(int migrantSize); 
//	{
//	
//		BasePopulation pop = new BasePopulation( (ArrayList<Chromosome>) individuals.clone());
//		pop.sortChromosomes();
//		
//		PopulationC migrantPop = new PopulationC(migrantSize,false);
//		for(int i = 0; i < migrantSize; i++){
//			migrantPop.saveChromosome(pop.getChromosome(i));
//		}
//		
//		return migrantPop;
//	}
	
	
	public void removeWeakest(){
		individuals.remove(getWeakestIndex());
	}
	
	public void integrateMigrants(BasePopulation immigrantsPop){
		for(int i = 0; i < immigrantsPop.size(); i++){
			removeWeakest();
		}
		
		for(int i = 0; i < immigrantsPop.size(); i++){
			saveChromosome(immigrantsPop.getChromosome(i));
		}
	}
	
	public double totalFitness(){
		double fitness =0;
		for(BaseChromosome individual:individuals){
			fitness += individual.getFitness();
		}
		return fitness;
	}
	private void sortChromosomes(){
		Collections.sort(individuals, BaseChromosome.SORT_DECREASING_ORDER);
	}
	
	
	public int compareTo(BasePopulation other) {
		return (int)(totalFitness() - other.totalFitness());
	}

}
