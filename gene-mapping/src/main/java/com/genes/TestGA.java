package com.genes;

import com.dic.distributedga.core.Population;

public class TestGA {

	public static void main(String[] args){
		Population pop = new Population(3, true);
		System.out.println("size of population "+pop.size());
	}
}
