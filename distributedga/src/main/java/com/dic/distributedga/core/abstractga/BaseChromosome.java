package com.dic.distributedga.core.abstractga;

import java.lang.reflect.Array;
import java.util.Comparator;

public abstract class BaseChromosome {
	BaseGene[] genes;
	int geneLength;
	
	public BaseChromosome(Class<? extends BaseGene> geneClass, int geneLength) {
		this.genes = (BaseGene[]) Array.newInstance(geneClass, geneLength);
	}
	
	public int size(){
		return genes.length;
	}
	
	public BaseGene getGene(int index){
		return genes[index];
	}
	
	public void setGene(int index, BaseGene g){
		genes[index] = g;
	}
	
	public void generateChromosome(){
		for(int i =0; i < geneLength; i++){
			genes[i].setGene(genes[i].getRandomGene());			
		}
	}
	
	public abstract double getFitness();
	public abstract double getFitnessComparison(BaseChromosome c); 
	
	public static final Comparator<BaseChromosome> SORT_DECREASING_ORDER = new Comparator<BaseChromosome>() {

		public int compare(BaseChromosome o1, BaseChromosome o2) {
			
			return (int) (o2.getFitness() - o1.getFitness());
		}
	};
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(BaseGene gene:genes){
			sb.append(gene.getGene()+"");
		}
		return sb.toString();
	};
}
