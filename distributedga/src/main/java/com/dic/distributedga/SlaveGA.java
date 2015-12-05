package com.dic.distributedga;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.dic.distributedga.comm.GAWorkContext;
import com.dic.distributedga.comm.IReceiverListener;
import com.dic.distributedga.comm.Sender;
import com.dic.distributedga.core.Algorithm;
import com.dic.distributedga.core.FitnessCalc;
import com.dic.distributedga.core.Population;

public class SlaveGA {
	private Sender sender;

	private String masterIPAddr;
	private int portNo;
	private WorkerInfo serverInfo;
	private Population population;
	private Population migrantPopulation;
	private String optimalSol;
	private int generationCount;
	private volatile boolean terminate;

	private final Object waitObject = new Object();

	private IReceiverListener wstListener = new IReceiverListener() {

		public void migrantPopReceivedEvent(GAWorkContext gaWorkContext) {
			synchronized (waitObject) {
				Population newMigrant = gaWorkContext.getPopulation();
				if (migrantPopulation == null || migrantPopulation.compareTo(newMigrant) < 0) {
					migrantPopulation = newMigrant;
				}

				waitObject.notify();
			}
		}

		public void initialPopReceivedEvent(GAWorkContext gaWorkContext) {
			population = gaWorkContext.getPopulation();
			optimalSol = gaWorkContext.getSolution();
			FitnessCalc.setSolution(optimalSol);
			synchronized (waitObject) {
				waitObject.notify();
			}
		}

		public void terminationReceivedEvent(GAWorkContext gaWorkContext) {
			serverInfo.getWorkerSocketThread().setTerminate();
			terminate = true;
		}

		public void readyReceivedEvent(GAWorkContext gaWorkContext) {
			// TODO Auto-generated method stub

		}
	};

	public SlaveGA(String ipAddress, int portNo) {
		this.masterIPAddr = ipAddress;
		this.portNo = portNo;
	}

	public void init() {
		sender = new Sender();
	}

	public void start() {
		// First task tell master I am ready
		try {

			Socket workerSocket = Utils.createSocket(masterIPAddr, portNo);

			serverInfo = new WorkerInfo(workerSocket.getInetAddress().getHostAddress(), portNo);
			serverInfo.setSocket(workerSocket);

			WorkerSocketThread wst = new WorkerSocketThread(serverInfo.getSocket(), wstListener);
			serverInfo.setWorkerSocketThread(wst);

			wst.start();

			sender.sendReady(serverInfo.getSendStream());

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// now wait from server for initial population
		synchronized (waitObject) {
			while (population == null) {
				try {
					waitObject.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		// we got population to work on
		processPopulation();
	}

	public void processPopulation() {

		while (terminate == false && population.getFittest().getFitness() < FitnessCalc.getMaxFitness()) {
			generationCount++;
			/*if (generationCount % Utils.EVOLUTION_BATCH_SIZE == 0) {

				Population migrants = population.getFittestSubset(Utils.MIGRANT_SIZE);

				try {
					sender.migratePopulation(migrants, serverInfo.getSendStream());
				} catch (UnknownHostException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				synchronized (waitObject) {

					if (migrantPopulation == null) {
						try {
							waitObject.wait();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						population.integrateMigrants(migrantPopulation);
						migrantPopulation = null;
					}

				}
			}*/
			System.out.println("Generation: " + generationCount + " Fittest: " + population.getFittest().getFitness());
			population = Algorithm.evolvePopulation(population);
		}

		if (terminate == true) {
			
			System.out.println("Terminate received from master.");
		
		} else {
			
			Population bestPopulation = new Population(1, false);
			bestPopulation.saveIndividual(population.getFittest());
			try {
				sender.sendTerminateMsg(bestPopulation, serverInfo.getSendStream());
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

			System.out.println("Solution found!");
			System.out.println("Generation: " + generationCount);
			System.out.println("Genes:");
			System.out.println(population.getFittest());
		}
	}
}
