package com.genes;

import com.dic.distributedga.core.abstractga.BaseGene;

public class BinaryGene extends BaseGene{

	@Override
	public byte getRandomGene() {
		return (byte)(Math.random()>0.5?1:0);
	}
	
}
