package com.maxeval.maxeval;

import java.lang.reflect.InvocationTargetException;

import com.dic.distributedga.core.abstractga.BaseChromosome;
import com.dic.distributedga.core.abstractga.BaseGene;
import com.dic.distributedga.core.abstractga.BasePopulation;

public class BinaryPopulation extends BasePopulation{
	Class chromosomeClass;
	Class geneClass;
	int geneLength;	
	int popSize;
	public BinaryPopulation(Class<? extends BaseChromosome> chromosomeClass, Class<? extends BaseGene> geneClass, int populationSize, int geneLength, boolean initialise) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		super(chromosomeClass, geneClass, populationSize, geneLength, initialise);
		this.chromosomeClass = chromosomeClass;
		this.geneClass = geneClass;
		this.geneLength = geneLength;
		this.popSize = populationSize;
	}
	
	
	@Override
	public BasePopulation getPopulationSubset(int startIndex, int endIndex) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException  {
		int size = endIndex-startIndex+1;
		if(size <= 0)
			return null;
		BinaryPopulation p = new BinaryPopulation(chromosomeClass, geneClass, size, geneLength, false);
		for(int i = startIndex; i <= endIndex; i++)
			p.saveChromosome( getChromosome(i));
		return p;	
	}

	@Override
	public BasePopulation getFittestSubset(int migrantSize) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, CloneNotSupportedException {
		BinaryPopulation pop = new BinaryPopulation(chromosomeClass, geneClass, popSize, geneLength, false);
		for(int i =0;i<popSize;i++)
			pop.saveChromosome(getChromosome(i));
		pop.sortChromosomes();
		
		BinaryPopulation migrantPop = new BinaryPopulation(chromosomeClass, geneClass, migrantSize,geneLength, false);
		for(int i = 0; i < migrantSize; i++){
			migrantPop.saveChromosome(pop.getChromosome(i));
		}		
		return migrantPop;
	}

}
