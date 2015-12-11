package com.genes;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import com.dic.distributedga.core.abstractga.BaseChromosome;
import com.dic.distributedga.core.abstractga.BaseGene;
import com.dic.distributedga.core.abstractga.BasePopulation;

public class BinaryPopulation extends BasePopulation{
	Class chromosomeClass;
	Class geneClass;
	int geneLength;	
	
	public BinaryPopulation(Class<? extends BaseChromosome> chromosomeClass, Class<? extends BaseGene> geneClass, int populationSize, int geneLength, boolean initialise) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		super(chromosomeClass, geneClass, populationSize, geneLength, initialise);
		this.chromosomeClass = chromosomeClass;
		this.geneClass = geneClass;
		this.geneLength = geneLength;
		// TODO Auto-generated constructor stub
	}
	
	public BinaryPopulation(ArrayList<BaseChromosome> o_chromosomes) {
		super(o_chromosomes);
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
		BinaryPopulation pop = new BinaryPopulation( (ArrayList<BaseChromosome>) this.clone());
		pop.sortChromosomes();
		
		BinaryPopulation migrantPop = new BinaryPopulation(chromosomeClass, geneClass, migrantSize,geneLength, false);
		for(int i = 0; i < migrantSize; i++){
			migrantPop.saveChromosome(pop.getChromosome(i));
		}		
		return migrantPop;
	}

}
