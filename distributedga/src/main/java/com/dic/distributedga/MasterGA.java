package com.dic.distributedga;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import com.dic.distributedga.comm.GAWorkContext;
import com.dic.distributedga.comm.IReceiverListener;
import com.dic.distributedga.comm.Receiver;
import com.dic.distributedga.comm.Sender;
import com.dic.distributedga.core.abstractga.BasePopulation;
import com.dic.distributedga.core.abstractga.GAOperators;

public class MasterGA {

	private Sender sender;
	private Receiver receiver;
	private GAConfig gaConfig;
	private int listenPort;
	private int slavesCount;
	private int readySlavesCount;
	private volatile int message = 0;
	private final Object waitObject = new Object();
	private HashMap<String, WorkerInfo> workerTable;
	private HashMap<String, WorkerInfo> failedWorkerTable;
	private LinkedList<String> receivedMigrantsFrom;
	private BasePopulation optimalResult;
	private volatile boolean completedStatus;
	
	private IReceiverListener receiverListener = new IReceiverListener() {

		public void migrantPopReceivedEvent(String ipAddress, GAWorkContext gaWorkContext) {
			synchronized (waitObject) {
				WorkerInfo workerInfo = workerTable.get(ipAddress);
				workerInfo.setMigrantPopulation(gaWorkContext.getPopulation());

				if (!receivedMigrantsFrom.contains(ipAddress)) {
					receivedMigrantsFrom.add(ipAddress);
				}

				message |= Utils.MSG_MIGRATION_POP;
				waitObject.notify();
			}
		}

		public void initialPopReceivedEvent(String ipAddress, GAWorkContext gaWorkContext) {

		}

		public void terminationReceivedEvent(String ipAddress, GAWorkContext gaWorkContext) {
			synchronized (waitObject) {
				message |= Utils.MSG_TERMINATE;
				optimalResult = gaWorkContext.getPopulation();
				waitObject.notify();
			}

		}

		public void readyReceivedEvent(String ipAddress, GAWorkContext gaWorkContext) {

			synchronized (waitObject) {
				readySlavesCount += 1;
				waitObject.notify();
			}
		}
	};

	public MasterGA(GAConfig gaConfig) {
		this.gaConfig = gaConfig;
		this.slavesCount = gaConfig.getContainersCount();
		this.listenPort = gaConfig.getPortNo();
	}

	public void init() {
		try {
			workerTable = new HashMap<String, WorkerInfo>();
			failedWorkerTable = new HashMap<String, WorkerInfo>();
			receiver = new Receiver(listenPort, 0);
			sender = new Sender();
			receivedMigrantsFrom = new LinkedList<String>();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void start() throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		try {

			for (int i = 0; i < slavesCount; i++) {
				Socket workerSocket = receiver.startListening();
				workerSocket.setTcpNoDelay(true);

				WorkerInfo worker = new WorkerInfo(workerSocket.getInetAddress().getHostAddress(), listenPort);
				worker.setSocket(workerSocket);

				WorkerSocketThread wst = new WorkerSocketThread(worker.getSocket(), receiverListener);
				worker.setWorkerSocketThread(wst);

				workerTable.put(worker.getIpAddress(), worker);

				wst.start();

			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		// TILL NOW ALL SLAVES HAVE CONNECTED TO MASTER
		// wait for them to send ready
		synchronized (waitObject) {
			while (readySlavesCount != slavesCount) {
				try {
					waitObject.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		// now create population and send to slaves
		startAndDistributeGA();
	}

	private void startAndDistributeGA() throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {

		GAOperators gaOperator = (GAOperators)gaConfig.getDerGAOperators().newInstance();
		BasePopulation startPop = gaOperator.initStartPopulation();
	
		int startPopSize = startPop.size();
		int step = startPopSize/slavesCount;
		BasePopulation[] partitionedPop = new BasePopulation[slavesCount];
		
		for(int i=0;i <slavesCount-1;i++){
			partitionedPop[i] = startPop.getPopulationSubset(i*step, (i+1)*step-1);
		}
		partitionedPop[slavesCount-1] = startPop.getPopulationSubset((slavesCount-1)*step, startPopSize-1);
		
		Iterator<WorkerInfo> iterator = workerTable.values().iterator();
		int i = 0;
		while (iterator.hasNext()) {
			WorkerInfo worker = iterator.next();
			try {
				sender.sendInitPopulation(partitionedPop[i], worker.getSendStream());
				worker.setPoplulation(partitionedPop[i]);
				i++;
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		synchronized (waitObject) {
			while (true) {
				try {
					waitObject.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				if ((message & Utils.MSG_TERMINATE) == Utils.MSG_TERMINATE) {
					// send all clients terminate, we got solution.
					for (WorkerInfo workerInfo : workerTable.values()) {
						try {
							sender.sendTerminateMsg(null, workerInfo.getSendStream());
						} catch (UnknownHostException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

					}
					message &= ~Utils.MSG_TERMINATE;
					break;
				}

				if ((message & Utils.MSG_MIGRATION_POP) == Utils.MSG_MIGRATION_POP) {
					
					while(receivedMigrantsFrom.size()>1){
						
						String firstNodeIp = receivedMigrantsFrom.removeFirst();
						String secondNodeIp = receivedMigrantsFrom.removeFirst();
						
						WorkerInfo firstWorker = workerTable.get(firstNodeIp);
						WorkerInfo secondWorker = workerTable.get(secondNodeIp);
						
						try {
							sender.migratePopulation(firstWorker.getMigrantPopulation(), secondWorker.getSendStream());
							firstWorker.setMigrantPopulation(null);
						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
						
						try {
							sender.migratePopulation(secondWorker.getMigrantPopulation(), firstWorker.getSendStream());
							secondWorker.setMigrantPopulation(null);
						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					
					if(receivedMigrantsFrom.size() == 0)
						message &= ~Utils.MSG_MIGRATION_POP;
				}
			}
		}
		completedStatus = true;
		System.out.println("Best solution found :");
		System.out.println(optimalResult.getFittest());

	}
	public boolean isCompleted(){
		return completedStatus;
	}
}
