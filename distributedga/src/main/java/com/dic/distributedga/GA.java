package com.dic.distributedga;
import com.dic.distributedga.core.Algorithm;
import com.dic.distributedga.core.FitnessCalc;
import com.dic.distributedga.core.Individual;
import com.dic.distributedga.core.Population;

public class GA {

    public static void main(String[] args) {
    	/*TODO:
    	 * Phase 1
    	 * Write listener and sender class using sockets.
    	 * Sender will be master to send some initial data
    	 * Listeners will wait for command from sender for initial population.
    	 * Listeners will start there calculation and dump result.
    	 * 
    	 * 
    	 * Phase 2. 
    	 * Listeners will send back there result to sender.
    	 * Sender will chose best answer and display.
    	 * 
    	 * Phase 3.
    	 * At periodic interval, sender will send some individuals to listeners.
    	 * Listeners will add that data in there next round of calculation.
    	 * 
    	 * Phase 4.
    	 * Sender will send initial population.
    	 * Sender will send list of all listeners.
    	 * At periodic interval, listeners will send each other some data.
    	 * Listener receiving data from other listener will use it like in phase 3.
    	 * In end, all Listeners will send back there best solution to sender
    	 * Sender will display data.
    	 */
    	
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