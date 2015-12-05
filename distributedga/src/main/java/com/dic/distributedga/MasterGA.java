package com.dic.distributedga;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import com.dic.distributedga.comm.GAWorkContext;
import com.dic.distributedga.comm.IReceiverListener;
import com.dic.distributedga.comm.Receiver;
import com.dic.distributedga.comm.Sender;
import com.dic.distributedga.core.Population;

public class MasterGA {

	private Sender sender;
	private Receiver receiver;
	private int listenPort;
	private int slavesCount;
	private int readySlavesCount;
	private volatile int message = 0;
	private final Object waitObject = new Object();
	private HashMap<String, WorkerInfo> workerTable;
	private LinkedList<String> receivedMigrantsFrom;
	private Population optimalResult;
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

	public MasterGA(int slavesCount, int portNo) {
		this.slavesCount = slavesCount;
		this.listenPort = portNo;
	}

	public MasterGA(String[] slaveIPs, int portNo) {
		this.slavesCount = slaveIPs.length;
		this.listenPort = portNo;

		/*
		 * for(int i=0;i<slavesCount;i++){ WorkerInfo worker = new
		 * WorkerInfo(slaveIPs[i],portNo); workerTable.put(slaveIPs[i], worker);
		 * }
		 */
	}

	public void init() {
		try {
			workerTable = new HashMap<String, WorkerInfo>();
			receiver = new Receiver(listenPort, 0);
			sender = new Sender();
			receivedMigrantsFrom = new LinkedList<String>();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void start() {
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

	private void startAndDistributeGA() {
		String soln = "111000000000000000000000000000000000000000000000000000000001111";

		Population aPop = new Population(50, true);

		// break pop into 2 parts. send to 2 clients. 6790, 6791 port
		Population[] p = new Population[2];
		p[0] = aPop.getPopulationSubset(0, 24);
		p[1] = aPop.getPopulationSubset(25, 49);

		Iterator<WorkerInfo> iterator = workerTable.values().iterator();
		int i = 0;
		while (iterator.hasNext()) {
			WorkerInfo worker = iterator.next();
			try {
				sender.sendInitPopulation(p[i++], soln, worker.getSendStream());
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

		System.out.println("Best solution found :");
		System.out.println(optimalResult.getFittest());

	}
}
