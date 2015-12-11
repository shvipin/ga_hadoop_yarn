package com.dic.distributedga.core.abstractga;

import java.io.Serializable;

public abstract class BaseGene implements Serializable {
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




