package com.maxeval.maxeval;

import com.dic.distributedga.core.abstractga.BaseChromosome;
import com.dic.distributedga.core.abstractga.BaseGene;

public class BinaryChromosome extends BaseChromosome{

	public BinaryChromosome(Class<? extends BaseGene> geneClass, int geneLength) throws InstantiationException, IllegalAccessException {
		super(geneClass, geneLength);
		// TODO Auto-generated constructor stub
	}
	
	public BinaryChromosome(BinaryGene[] genes) {
		super(genes);
	}

	@Override
	public double getFitness() {
		return FitnessEvaluator.getInstance().getFitness(this);
	}

	@Override
	public double getFitnessComparison(BaseChromosome c) {
		return this.getFitness() - c.getFitness();		
	}

}
