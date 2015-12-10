package com.dic.distributedga.core.abstractga;

public abstract class BaseGene {
	byte gene;
	
	public void setGene(byte g){
		this.gene = g;
	}
	public byte getGene(){
		return this.gene;
	}	
	public void applyMutation() {
		setGene(getRandomGene());
	}
	public abstract byte getRandomGene();	
	
}




