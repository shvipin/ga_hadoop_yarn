package com.dic.distributedga;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.dic.distributedga.comm.GAWorkContext;
import com.dic.distributedga.comm.IReceiverListener;
import com.dic.distributedga.comm.Sender;
import com.dic.distributedga.core.abstractga.BasePopulation;
import com.dic.distributedga.core.abstractga.GAOperators;

public class SlaveGA {
	private Sender sender;

	private String masterIPAddr;
	private int portNo;
	private WorkerInfo serverInfo;
	private BasePopulation population;
	private BasePopulation migrantBasePopulation;
	private int generationCount;
	private volatile boolean terminate;
	private GAConfig gaConfig;
	private final Object waitObject = new Object();

	private Options opts;

	private IReceiverListener wstListener = new IReceiverListener() {

		public void migrantPopReceivedEvent(String ipAddress, GAWorkContext gaWorkContext) {
			synchronized (waitObject) {
				BasePopulation newMigrant = gaWorkContext.getPopulation();
				if (migrantBasePopulation == null || migrantBasePopulation.compareTo(newMigrant) < 0) {
					migrantBasePopulation = newMigrant;
				}

				waitObject.notify();
			}
		}

		public void initialPopReceivedEvent(String ipAddress, GAWorkContext gaWorkContext) {
			population = gaWorkContext.getPopulation();
			synchronized (waitObject) {
				waitObject.notify();
			}
		}

		public void terminationReceivedEvent(String ipAddress, GAWorkContext gaWorkContext) {
			serverInfo.getWorkerSocketThread().setTerminate();
			terminate = true;
		}

		public void readyReceivedEvent(String ipAddress, GAWorkContext gaWorkContext) {

		}
	};

	public SlaveGA() {
		opts = new Options();
		opts.addOption(Utils.CMD_ARG_AM_CONTAINER_MEM, true, "Amount of memory in MB for slave containers");
		opts.addOption(Utils.CMD_ARG_HELP, false, "Print usage");
		opts.addOption(Utils.CMD_ARG_NM_DER_POP_CLS, true, "derived population class name with package");
		opts.addOption(Utils.CMD_ARG_NM_DER_CHROMOSOME_CLS, true, "derived chromosome class name with package");
		opts.addOption(Utils.CMD_ARG_NM_DER_GENE_CLS, true, " derived gene class name with package");
		opts.addOption(Utils.CMD_ARG_NM_DER_GAOPERATOR_CLS, true, "derived gaoperator class name with package");
		opts.addOption(Utils.CMD_ARG_NM_PORT, true,
				"port number on which communication will take place in distirbuted ga app");
	}

	public void init(String[] args) throws ParseException, ClassNotFoundException {

		sender = new Sender();

		CommandLine cliParser = new GnuParser().parse(opts, args);

		if (!cliParser.hasOption(Utils.CMD_ARG_NM_SERVER_IP)) {
			throw new IllegalArgumentException("Server ip not provided");
		}


		if (!cliParser.hasOption(Utils.CMD_ARG_AM_DER_POP_CLS)) {
			throw new IllegalArgumentException("Derived class for BasePopulation not provided");
		}
		if (!cliParser.hasOption(Utils.CMD_ARG_AM_DER_CHROMOSOME_CLS)) {
			throw new IllegalArgumentException("Derived class for BaseChromosome not provided");
		}
		if (!cliParser.hasOption(Utils.CMD_ARG_AM_DER_GENE_CLS)) {
			throw new IllegalArgumentException("Derived class for BaseGene not provided");
		}
		if (!cliParser.hasOption(Utils.CMD_ARG_AM_DER_GAOPERATOR_CLS)) {
			throw new IllegalArgumentException("Derived class for BaseOperator not provided");
		}

		gaConfig.setDerPopulation(Class.forName(cliParser.getOptionValue(Utils.CMD_ARG_AM_DER_POP_CLS)));
		gaConfig.setDerChromosome(Class.forName(cliParser.getOptionValue(Utils.CMD_ARG_AM_DER_CHROMOSOME_CLS)));
		gaConfig.setDerGene(Class.forName(cliParser.getOptionValue(Utils.CMD_ARG_AM_DER_GENE_CLS)));
		gaConfig.setDerGAOperators(Class.forName(cliParser.getOptionValue(Utils.CMD_ARG_AM_DER_GAOPERATOR_CLS)));

		this.masterIPAddr = cliParser.getOptionValue(Utils.CMD_ARG_NM_SERVER_IP);

		if (cliParser.hasOption(Utils.CMD_ARG_AM_PORT)) {
			gaConfig.setPortNo(Integer.parseInt(cliParser.getOptionValue(Utils.CMD_ARG_AM_PORT)));
			this.portNo = gaConfig.getPortNo();
		}

	}

	public void printUsage(Options opts) {
		new HelpFormatter().printHelp("ApplicationMaster", opts);
	}

	public void start() throws InstantiationException, IllegalAccessException, NoSuchMethodException, SecurityException,
			IllegalArgumentException, InvocationTargetException {
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
		processBasePopulation();
	}

	public void processBasePopulation() throws NoSuchMethodException, SecurityException, InstantiationException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		GAOperators gaOperator = (GAOperators) gaConfig.getDerGAOperators().newInstance();

		while (terminate == false && gaOperator.isConvergenceReached(population)) {
			generationCount++;
			if (generationCount % Utils.EVOLUTION_BATCH_SIZE == 0) {

				BasePopulation migrants = population.getFittestSubset(Utils.MIGRANT_SIZE);

				try {
					sender.migratePopulation(migrants, serverInfo.getSendStream());
				} catch (UnknownHostException e1) {
					e1.printStackTrace();
				} catch (IOException e1) {
					e1.printStackTrace();
				}

				synchronized (waitObject) {

					if (migrantBasePopulation == null) {
						try {
							waitObject.wait();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						population.integrateMigrants(migrantBasePopulation);
						migrantBasePopulation = null;
					}

				}
			}
			System.out.println("Generation: " + generationCount + " Fittest: " + population.getFittest().getFitness());
			population = gaOperator.evolvePopulation(population);
		}

		if (terminate == true) {

			System.out.println("Terminate received from master.");

		} else {

			int geneLength = population.getChromosome(0).size();

			Class[] argType = { Class.class, Class.class, Integer.class, Integer.class, Boolean.class };
			Object[] args = { gaConfig.getDerChromosome(), gaConfig.getDerGene(), new Integer(1),
					new Integer(geneLength), new Boolean(false) };
			Constructor constructor = gaConfig.getDerPopulation().getConstructor(argType);

			BasePopulation bestBasePopulation = (BasePopulation) constructor.newInstance(args);

			bestBasePopulation.saveChromosome(population.getFittest());
			try {
				sender.sendTerminateMsg(bestBasePopulation, serverInfo.getSendStream());
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

	public static void main(String[] args) {
		System.out.println("starting ga class");
		for (int i = 0; i < args.length; i++) {
			System.out.println("argument " + i + " = " + args[i]);
		}
		try {
			InetAddress address = InetAddress.getLocalHost();
			System.out.println(address.getHostName() + " " + address.getHostAddress());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			SlaveGA slaveGA = new SlaveGA();
			slaveGA.init(args);
			slaveGA.start();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		
		System.out.println("exiting");

	}

}
