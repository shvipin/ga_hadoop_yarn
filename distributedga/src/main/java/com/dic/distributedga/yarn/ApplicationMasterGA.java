package com.dic.distributedga.yarn;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;

import com.dic.distributedga.GAConfig;
import com.dic.distributedga.MasterGA;
import com.dic.distributedga.Utils;

public class ApplicationMasterGA {

	private static final Log log = LogFactory.getLog(ApplicationMasterGA.class);
	private static final String log4jPath = "log4j.properties";
	private static final String GA_JAR_STRING_PATH = Client.USER_GA_JAR_NAME;
	private Configuration conf;
	private Options opts;
	private ApplicationAttemptId appAttemptId;
	private int containerMemory;
	private int containerVirtualCores;
	private int numTotalContainers;
	private String userGAJarHDFSLoc;
	private long userGAJarHDFSLen;
	private long userGAJarHDFSTimeStamp;
	private ByteBuffer allTokens;
	private UserGroupInformation appSubmitterUgi;
	private AMRMClientAsync amRmClient;
	private NMClientAsync amNmClientAsync;
	private AMNMCallbackHandler amNmCallbackHandler;
	private TimelineClient timelineClient;
	private String appMasterHostname;
	private int appMasterRpcPort;
	private String appMasterTrackingUrl;
	private AtomicInteger numRequestedContainers;
	private AtomicInteger numCompletedContainers;
	private AtomicInteger numAllocatedContainers;
	private AtomicInteger numFailedContainers;
	private volatile boolean done;
	private List<Thread> launchThreads;
	
	private GAConfig gaConfig;

	public static enum DSEvent {
		DS_APP_ATTEMPT_START, DS_APP_ATTEMPT_END, DS_CONTAINER_START, DS_CONTAINER_END
	}

	public static enum DSEntity {
		DS_APP_ATTEMPT, DS_CONTAINER
	}

	private class AMRMCallbackHandler implements AMRMClientAsync.CallbackHandler {

		public float getProgress() {
			float progress = (float) numCompletedContainers.get() / numTotalContainers;
			return progress;
		}

		public void onContainersAllocated(List<Container> allocatedContainers) {

			log.info("Got response from RM for containers asked, allocated cnt " + allocatedContainers.size());

			numAllocatedContainers.addAndGet(allocatedContainers.size());

			for (Container allocatedContainer : allocatedContainers) {
				log.info("Launching shell command on a new container." + ", containerId=" + allocatedContainer.getId()
						+ ", containerNode=" + allocatedContainer.getNodeId().getHost() + ":"
						+ allocatedContainer.getNodeId().getPort() + ", containerNodeURI="
						+ allocatedContainer.getNodeHttpAddress() + ", containerResourceMemory"
						+ allocatedContainer.getResource().getMemory() + ", containerResourceVirtualCores"
						+ allocatedContainer.getResource().getVirtualCores());
				LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer,
						amNmCallbackHandler);
				Thread launchThread = new Thread(runnableLaunchContainer);
				launchThreads.add(launchThread);
				launchThread.start();
			}
		}

		public void onContainersCompleted(List<ContainerStatus> completedContainers) {
			log.info("got response from RM for container completed = " + completedContainers.size());

			for (ContainerStatus containerStatus : completedContainers) {
				log.info(appAttemptId + " got container status for containerID=" + containerStatus.getContainerId()
						+ ", state=" + containerStatus.getState() + ", exitStatus=" + containerStatus.getExitStatus()
						+ ", diagnostics=" + containerStatus.getDiagnostics());

				int exitStatus = containerStatus.getExitStatus();
				if (exitStatus != 0) {
					if (ContainerExitStatus.ABORTED != exitStatus) {
						numCompletedContainers.incrementAndGet();
						numFailedContainers.incrementAndGet();
					} else {
						numAllocatedContainers.decrementAndGet();
						numRequestedContainers.decrementAndGet();
					}
				} else {
					numCompletedContainers.incrementAndGet();
					log.info("successfuly completed container id = " + containerStatus.getContainerId());
				}

				if (timelineClient != null) {
					publishContainerEndEvent(timelineClient, containerStatus, appSubmitterUgi);
				}
			}

			int remCount = numTotalContainers - numRequestedContainers.get();
			numRequestedContainers.addAndGet(remCount);

			if (remCount > 0) {
				for (int i = 0; i < remCount; i++) {
					ContainerRequest containerAsk = setupContainerAskForRM();
					amRmClient.addContainerRequest(containerAsk);
				}
			}

			if (numCompletedContainers.get() == numTotalContainers) {
				done = true;
			}
		}

		public void onError(Throwable arg0) {
			done = true;
			amRmClient.stop();
		}

		public void onNodesUpdated(List<NodeReport> arg0) {
			// ignore it for now
		}

		public void onShutdownRequest() {
			done = true;
		}

	}

	static class AMNMCallbackHandler implements NMClientAsync.CallbackHandler {

		private ConcurrentMap<ContainerId, Container> containers;
		private final ApplicationMasterGA applicationMasterGA;

		public AMNMCallbackHandler(ApplicationMasterGA applicationMasterGA) {
			this.applicationMasterGA = applicationMasterGA;
			containers = new ConcurrentHashMap<ContainerId, Container>();
		}

		public void addContainer(ContainerId containerId, Container container) {
			containers.putIfAbsent(containerId, container);
		}

		public void onContainerStarted(ContainerId arg0, Map<String, ByteBuffer> arg1) {
			// TODO Auto-generated method stub

		}

		public void onContainerStatusReceived(ContainerId arg0, ContainerStatus arg1) {
			// TODO Auto-generated method stub

		}

		public void onContainerStopped(ContainerId arg0) {
			// TODO Auto-generated method stub

		}

		public void onGetContainerStatusError(ContainerId arg0, Throwable arg1) {
			// TODO Auto-generated method stub

		}

		public void onStartContainerError(ContainerId arg0, Throwable arg1) {
			// TODO Auto-generated method stub

		}

		public void onStopContainerError(ContainerId arg0, Throwable arg1) {
			// TODO Auto-generated method stub

		}

	}

	private class LaunchContainerRunnable implements Runnable {

		Container container;
		AMNMCallbackHandler amNmCallbackHandler;

		public LaunchContainerRunnable(Container allocatedContainer, AMNMCallbackHandler amNmCallbackHandler) {
			this.container = allocatedContainer;
			this.amNmCallbackHandler = amNmCallbackHandler;
		}

		public void run() {
			log.info("Setting up container launch container for container id= " + container.getId());

			Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

			Path path = new Path(userGAJarHDFSLoc);
			URL yarnUrl = null;
			try {
				yarnUrl = ConverterUtils.getYarnUrlFromURI(new URI(path.toString()));
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}

			LocalResource gaJarRes = LocalResource.newInstance(yarnUrl, LocalResourceType.FILE,
					LocalResourceVisibility.APPLICATION, userGAJarHDFSLen, userGAJarHDFSTimeStamp);

			localResources.put(GA_JAR_STRING_PATH, gaJarRes);

			// setup java executable command for worker nodes.
			ArrayList<CharSequence> vargs = new ArrayList<CharSequence>();
			log.info("set up master command");
			vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
			vargs.add("-Xms" + containerMemory + "m");
			vargs.add("-cp");
			vargs.add(GA_JAR_STRING_PATH);
			vargs.add("com.dic.distributedga.SlaveGA");
			vargs.add("--"+Utils.CMD_ARG_NM_DER_POP_CLS +" "+ gaConfig.getDerPopulation().getCanonicalName());
			vargs.add("--"+Utils.CMD_ARG_NM_DER_CHROMOSOME_CLS +" "+ gaConfig.getDerChromosome().getCanonicalName());
			vargs.add("--"+Utils.CMD_ARG_NM_DER_GENE_CLS +" "+ gaConfig.getDerGene().getCanonicalName());
			vargs.add("--"+Utils.CMD_ARG_NM_DER_GAOPERATOR_CLS +" "+ gaConfig.getDerGAOperators().getCanonicalName());
			
			String ip = System.getenv(Environment.NM_HOST.name());
			try {
				InetAddress address = InetAddress.getByName(ip);
				ip = address.getHostAddress();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			vargs.add("--"+Utils.CMD_ARG_NM_SERVER_IP+" "+ip);
			vargs.add("--"+Utils.CMD_ARG_NM_PORT+" "+gaConfig.getPortNo());

			vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/SlaveGA.stdout");
			vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/SlaveGA.stderr");

			StringBuilder command = new StringBuilder();
			for (CharSequence str : vargs) {
				command.append(str).append(" ");
			}
			log.info("slave setup command is = " + command.toString());
			List<String> commands = new ArrayList<String>();
			commands.add(command.toString());

			ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(localResources, null, commands, null,
					allTokens.duplicate(), null);
			amNmCallbackHandler.addContainer(container.getId(), container);
			amNmClientAsync.startContainerAsync(container, ctx);
		}

	}

	public static void main(String[] args) {
		boolean result = false;
		ApplicationMasterGA appMaster = new ApplicationMasterGA();

		try {
			log.info("initialize application master");
			boolean doRun = appMaster.init(args);
			if (!doRun) {
				System.exit(0);
			}
			appMaster.run();
			result = appMaster.finish();

		} catch (ParseException e) {
			log.fatal("Error running AM");
			e.printStackTrace();
			LogManager.shutdown();
			ExitUtil.terminate(1, e);
		} catch (IOException e) {
			log.fatal("Error running AM");
			e.printStackTrace();
			LogManager.shutdown();
			ExitUtil.terminate(1, e);
		} catch (YarnException e) {
			log.fatal("Error running AM");
			e.printStackTrace();
			LogManager.shutdown();
			ExitUtil.terminate(1, e);
		} catch (ClassNotFoundException e) {
			log.fatal("Error running AM");
			e.printStackTrace();
			LogManager.shutdown();
			ExitUtil.terminate(1, e);
		}

		if (result) {
			log.info("Application master succesfully completed. exiting");
			System.exit(0);
		} else {
			log.info("Application master failed. exiting");
			System.exit(1);
		}
	}

	public ApplicationMasterGA() {
		userGAJarHDFSLoc = "";
		appMasterHostname = "";
		appMasterRpcPort = -1;
		appMasterTrackingUrl = "";
		launchThreads = new ArrayList<Thread>();
		numAllocatedContainers = new AtomicInteger();
		numCompletedContainers = new AtomicInteger();
		numRequestedContainers = new AtomicInteger();
		numFailedContainers = new AtomicInteger();

		conf = new YarnConfiguration();
		gaConfig = new GAConfig();
		
		opts = new Options();
		opts.addOption(Utils.CMD_ARG_AM_CONTAINER_MEM, true, "Amount of memory in MB for slave containers");
		opts.addOption(Utils.CMD_ARG_AM_CONTAINER_VCORE, true, "No. of virtual cores for slave containers");
		opts.addOption(Utils.CMD_ARG_AM_CONTAINER_NUM, true, "No of slave containers");
		opts.addOption(Utils.CMD_ARG_HELP, false, "Print usage");
		opts.addOption(Utils.CMD_ARG_AM_DER_POP_CLS, true, "derived population class name with package");
		opts.addOption(Utils.CMD_ARG_AM_DER_CHROMOSOME_CLS, true, "derived chromosome class name with package");
		opts.addOption(Utils.CMD_ARG_AM_DER_GENE_CLS, true, " derived gene class name with package");
		opts.addOption(Utils.CMD_ARG_AM_DER_GAOPERATOR_CLS, true, "derived gaoperator class name with package");
		opts.addOption(Utils.CMD_ARG_AM_PORT, true, "port number on which communication will take place in distirbuted ga app");
		
	}

	public boolean init(String[] args) throws ParseException, ClassNotFoundException {
		CommandLine cliParser = new GnuParser().parse(opts, args);

		if (args.length == 0) {
			printUsage(opts);
			throw new IllegalArgumentException("No args specified for application master to initialize");
		}

		if (fileExist(log4jPath)) {
			try {
				Utils.updateLog4jConfiguration(ApplicationMasterGA.class, log4jPath);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if (cliParser.hasOption("help")) {
			printUsage(opts);
			return false;
		}

		if (cliParser.hasOption("debug")) {
			dumpOutDebugInfo();
		}

		Map<String, String> envs = System.getenv();

		ContainerId containerId = ConverterUtils.toContainerId(envs.get(Environment.CONTAINER_ID.name()));
		appAttemptId = containerId.getApplicationAttemptId();

		if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
			throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV + " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_HOST.name())) {
			throw new RuntimeException(Environment.NM_HOST.name() + " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
			throw new RuntimeException(Environment.NM_HTTP_PORT + " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_PORT.name())) {
			throw new RuntimeException(Environment.NM_PORT.name() + " not set in the environment");
		}

		log.info("Application master for app" + ", appId=" + appAttemptId.getApplicationId().getId()
				+ ", clustertimestamp=" + appAttemptId.getApplicationId().getClusterTimestamp() + ", attemptId="
				+ appAttemptId.getAttemptId());

		containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
		containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
		numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));

		if (numTotalContainers <= 0) {
			throw new IllegalArgumentException("Invalid number of container for running this app");
		}
		gaConfig.setContainersCount(numTotalContainers);
		if (envs.get(Utils.USER_GA_JAR_HDFS_LOC).isEmpty()) {
			throw new IllegalArgumentException("No ga jar given in arguments.");
		}

		if(!cliParser.hasOption(Utils.CMD_ARG_AM_DER_POP_CLS)){
			throw new IllegalArgumentException("Derived class for BasePopulation not provided");
		}
		if(!cliParser.hasOption(Utils.CMD_ARG_AM_DER_CHROMOSOME_CLS)){
			throw new IllegalArgumentException("Derived class for BaseChromosome not provided");
		}
		if(!cliParser.hasOption(Utils.CMD_ARG_AM_DER_GENE_CLS)){
			throw new IllegalArgumentException("Derived class for BaseGene not provided");
		}
		if(!cliParser.hasOption(Utils.CMD_ARG_AM_DER_GAOPERATOR_CLS)){
			throw new IllegalArgumentException("Derived class for BaseOperator not provided");
		}
		
		gaConfig.setDerPopulation(Class.forName(cliParser.getOptionValue(Utils.CMD_ARG_AM_DER_POP_CLS)));
		gaConfig.setDerChromosome(Class.forName(cliParser.getOptionValue(Utils.CMD_ARG_AM_DER_CHROMOSOME_CLS)));
		gaConfig.setDerGene(Class.forName(cliParser.getOptionValue(Utils.CMD_ARG_AM_DER_GENE_CLS)));
		gaConfig.setDerGAOperators(Class.forName(cliParser.getOptionValue(Utils.CMD_ARG_AM_DER_GAOPERATOR_CLS)));
		
		if(cliParser.hasOption(Utils.CMD_ARG_AM_PORT)){
			gaConfig.setPortNo(Integer.parseInt(cliParser.getOptionValue(Utils.CMD_ARG_AM_PORT)));
		}

		userGAJarHDFSLoc = envs.get(Utils.USER_GA_JAR_HDFS_LOC);

		if (envs.containsKey(Utils.USER_GA_JAR_HDFS_LEN)) {
			userGAJarHDFSLen = Long.parseLong(envs.get(Utils.USER_GA_JAR_HDFS_LEN));
		}

		if (envs.containsKey(Utils.USER_GA_JAR_HDFS_TIMESTAMP)) {
			userGAJarHDFSTimeStamp = Long.parseLong(envs.get(Utils.USER_GA_JAR_HDFS_TIMESTAMP));
		}

		if (userGAJarHDFSLoc.isEmpty() || userGAJarHDFSLen <= 0 || userGAJarHDFSTimeStamp <= 0) {
			log.error("Illegal values in env for ga jar path =  " + userGAJarHDFSLoc + ", len = " + userGAJarHDFSLen
					+ ", timestamp = " + userGAJarHDFSTimeStamp);
			throw new IllegalArgumentException(
					"Illegal values of user ga jar loc, len " + "and time stamp in env variable");
		}

		return true;
	}

	public void printUsage(Options opts) {
		new HelpFormatter().printHelp("ApplicationMaster", opts);
	}

	public boolean fileExist(String hdfsPath) {
		return new File(hdfsPath).exists();
	}

	private void dumpOutDebugInfo() {
		log.info("Dump debug output");
		Map<String, String> envs = System.getenv();
		for (Map.Entry<String, String> env : envs.entrySet()) {
			log.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
			System.out.println("System env: key=" + env.getKey() + ", val=" + env.getValue());
		}

		BufferedReader buf = null;
		try {
			String lines = Shell.execCommand("ls", "-al");
			buf = new BufferedReader(new StringReader(lines));
			String line = "";
			while ((line = buf.readLine()) != null) {
				log.info("System CWD content: " + line);
				System.out.println("System CWD content: " + line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.cleanup(log, buf);
		}

	}

	public void run() throws IOException, YarnException {
		log.info("Starting application master");
		// ga = new MasterGA(numTotalContainers, 6000);
		new Thread(new Runnable() {

			public void run() {
				MasterGA ga = new MasterGA(gaConfig);
				ga.init();
				try {
					ga.start();
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}).start();

		Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
		DataOutputBuffer dob = new DataOutputBuffer();

		credentials.writeTokenStorageToStream(dob);

		// remove am->rm token so that containers cannot access it.
		Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
		while (iter.hasNext()) {
			Token<?> token = iter.next();
			log.info(token);
			if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
				iter.remove();
			}
		}

		allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

		String appSubmitterUserName = System.getenv(ApplicationConstants.Environment.USER.name());
		log.info("app submitter user name = " + appSubmitterUserName);
		appSubmitterUgi = UserGroupInformation.createRemoteUser(appSubmitterUserName);
		appSubmitterUgi.addCredentials(credentials);

		AMRMClientAsync.CallbackHandler amRmAllocListener = new AMRMCallbackHandler();
		amRmClient = AMRMClientAsync.createAMRMClientAsync(1000, amRmAllocListener);
		amRmClient.init(conf);
		amRmClient.start();

		amNmCallbackHandler = new AMNMCallbackHandler(this);
		amNmClientAsync = new NMClientAsyncImpl(amNmCallbackHandler);
		amNmClientAsync.init(conf);
		amNmClientAsync.start();

		startTimelineClient(conf);
		if (timelineClient != null) {
			publishApplicationAttemptEvent(timelineClient, appAttemptId.toString(), DSEvent.DS_APP_ATTEMPT_START,
					appSubmitterUgi);
		}

		// register self with resource manager
		// this will start heartbeating to rm.

		appMasterHostname = NetUtils.getHostname();
		RegisterApplicationMasterResponse response = amRmClient.registerApplicationMaster(appMasterHostname,
				appMasterRpcPort, appMasterTrackingUrl);

		int maxMem = response.getMaximumResourceCapability().getMemory();
		log.info("max mem capability of containers node " + maxMem);

		int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
		log.info("max virtual cores of container nodes " + maxVCores);

		// A resource ask cannot exceed the max.
		if (containerMemory > maxMem) {
			log.info("Container memory specified above max threshold of cluster." + " Using max value." + ", specified="
					+ containerMemory + ", max=" + maxMem);
			containerMemory = maxMem;
		}

		if (containerVirtualCores > maxVCores) {
			log.info("Container virtual cores specified above max threshold of cluster." + " Using max value."
					+ ", specified=" + containerVirtualCores + ", max=" + maxVCores);
			containerVirtualCores = maxVCores;
		}

		for (int i = 0; i < numTotalContainers; i++) {
			ContainerRequest containerAsk = setupContainerAskForRM();
			amRmClient.addContainerRequest(containerAsk);
		}
		numRequestedContainers.set(numTotalContainers);

	}

	public void startTimelineClient(final Configuration conf) {
		appSubmitterUgi.doAs(new PrivilegedAction<Void>() {

			public Void run() {
				if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
						YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {

					timelineClient = TimelineClient.createTimelineClient();
					timelineClient.init(conf);
					timelineClient.start();
				} else {
					timelineClient = null;
					log.warn("Timeline service is not enabled");
				}
				return null;
			}
		});
	}

	private static void publishApplicationAttemptEvent(final TimelineClient timelineClient, String appAttemptId,
			DSEvent appEvent, UserGroupInformation ugi) {
		final TimelineEntity entity = new TimelineEntity();
		entity.setEntityId(appAttemptId);
		entity.setEntityType(DSEntity.DS_APP_ATTEMPT.toString());

		entity.addPrimaryFilter("user", ugi.getShortUserName());
		TimelineEvent event = new TimelineEvent();
		event.setEventType(appEvent.toString());
		event.setTimestamp(System.currentTimeMillis());
		entity.addEvent(event);

		try {
			timelineClient.putEntities(entity);
		} catch (YarnException e) {
			log.error("App Attempt " + (appEvent.equals(DSEvent.DS_APP_ATTEMPT_START) ? "start" : "end")
					+ " event could not be published for " + appAttemptId.toString(), e);
		} catch (IOException e) {
			log.error("App Attempt " + (appEvent.equals(DSEvent.DS_APP_ATTEMPT_START) ? "start" : "end")
					+ " event could not be published for " + appAttemptId.toString(), e);
		}

	}

	private static void publishContainerEndEvent(final TimelineClient timelineClient, ContainerStatus container,
			UserGroupInformation ugi) {
		final TimelineEntity entity = new TimelineEntity();
		entity.setEntityId(container.getContainerId().toString());
		entity.setEntityType(DSEntity.DS_CONTAINER.toString());
		entity.addPrimaryFilter("user", ugi.getShortUserName());

		TimelineEvent event = new TimelineEvent();
		event.setTimestamp(System.currentTimeMillis());
		event.setEventType(DSEvent.DS_CONTAINER_END.toString());
		event.addEventInfo("State", container.getState().name());
		event.addEventInfo("Exit Status", container.getExitStatus());

		entity.addEvent(event);
		try {
			timelineClient.putEntities(entity);
		} catch (YarnException e) {
			log.error("Container end event could not be published for " + container.getContainerId().toString(), e);
		} catch (IOException e) {
			log.error("Container end event could not be published for " + container.getContainerId().toString(), e);
		}
	}

	private static void publishApplicatoinAttemptEvent(final TimelineClient timelineClient, String appAttemptId,
			DSEvent appEvent, UserGroupInformation ugi) {
		final TimelineEntity entity = new TimelineEntity();
		entity.setEntityId(appAttemptId);
		entity.setEntityType(DSEntity.DS_APP_ATTEMPT.toString());
		entity.addPrimaryFilter("user", ugi.getShortUserName());
		TimelineEvent event = new TimelineEvent();
		event.setEventType(appEvent.toString());
		event.setTimestamp(System.currentTimeMillis());
		entity.addEvent(event);
		try {
			timelineClient.putEntities(entity);
		} catch (YarnException e) {
			log.error("App Attempt " + (appEvent.equals(DSEvent.DS_APP_ATTEMPT_START) ? "start" : "end")
					+ " event could not be published for " + appAttemptId.toString(), e);
		} catch (IOException e) {
			log.error("App Attempt " + (appEvent.equals(DSEvent.DS_APP_ATTEMPT_START) ? "start" : "end")
					+ " event could not be published for " + appAttemptId.toString(), e);
		}

	}

	private ContainerRequest setupContainerAskForRM() {
		Priority priority = Priority.newInstance(0);
		Resource capability = Resource.newInstance(containerMemory, containerVirtualCores);
		ContainerRequest request = new ContainerRequest(capability, null, null, priority);
		return request;
	}

	public boolean finish() {

		while (!done && (numCompletedContainers.get() != numTotalContainers)) {
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {
			}

			if (timelineClient != null) {
				publishApplicatoinAttemptEvent(timelineClient, appAttemptId.toString(), DSEvent.DS_APP_ATTEMPT_END,
						appSubmitterUgi);
			}
		}

		for (Thread launchThread : launchThreads) {
			try {
				launchThread.join(10000);
			} catch (InterruptedException e) {
				log.info("Exception thrown in thread join: " + e.getMessage());
				e.printStackTrace();
			}
		}

		// When the application completes, it should stop all running containers
		log.info("Application completed. Stopping running containers");
		amNmClientAsync.stop();

		// When the application completes, it should send a finish application
		// signal to the RM
		log.info("Application completed. Signalling finish to RM");

		FinalApplicationStatus appStatus;
		String appMessage = null;
		boolean success = true;
		if (numFailedContainers.get() == 0 && numCompletedContainers.get() == numTotalContainers) {
			appStatus = FinalApplicationStatus.SUCCEEDED;
		} else {
			appStatus = FinalApplicationStatus.FAILED;
			appMessage = "Diagnostics." + ", total=" + numTotalContainers + ", completed="
					+ numCompletedContainers.get() + ", allocated=" + numAllocatedContainers.get() + ", failed="
					+ numFailedContainers.get();
			log.info(appMessage);
			success = false;
		}
		try {
			amRmClient.unregisterApplicationMaster(appStatus, appMessage, null);
		} catch (YarnException ex) {
			log.error("Failed to unregister application", ex);
		} catch (IOException e) {
			log.error("Failed to unregister application", e);
		}

		amRmClient.stop();

		// Stop Timeline Client
		if (timelineClient != null) {
			timelineClient.stop();
		}

		return success;

	}
}
