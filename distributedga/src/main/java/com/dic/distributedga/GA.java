package com.dic.distributedga;
import com.dic.distributedga.core.Algorithm;
import com.dic.distributedga.core.FitnessCalc;
import com.dic.distributedga.core.Individual;
import com.dic.distributedga.core.Population;

public class GA {

    public static void main(String[] args) {

        // Set a candidate solution
        String sol = "111110101010100111111111111010";
       // sol = sol.substring(0,100);
        int len = sol.length();
        System.out.println(len);
        
        Individual.defaultGeneLength = len;
         FitnessCalc.setSolution(sol);

        // Create an initial population
        Population myPop = new Population(50, true);
        
        // Evolve our population until we reach an optimum solution
        int generationCount = 0;
        while (myPop.getFittest().getFitness() < FitnessCalc.getMaxFitness()) {
            generationCount++;
            System.out.println("Generation: " + generationCount + " Fittest: " + myPop.getFittest().getFitness());
            myPop = Algorithm.evolvePopulation(myPop);
        }
        System.out.println("Solution found!");
        System.out.println("Generation: " + generationCount);
        System.out.println("Genes:");
        System.out.println(myPop.getFittest());

    }
}