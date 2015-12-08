package com.dic.distributedga.yarn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
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

import com.dic.distributedga.Utils;

public class ApplicationMasterGA {

	private static final Log log = LogFactory.getLog(ApplicationMasterGA.class);
	private static final String log4jPath = "log4j.properties";
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
	private AtomicInteger numRequestedContainers = new AtomicInteger();
	
	public static enum DSEvent {
		DS_APP_ATTEMPT_START, DS_APP_ATTEMPT_END, DS_CONTAINER_START, DS_CONTAINER_END
	}

	public static enum DSEntity {
		DS_APP_ATTEMPT, DS_CONTAINER
	}
	private class AMRMCallbackHandler implements AMRMClientAsync.CallbackHandler {

		public float getProgress() {
			// TODO Auto-generated method stub
			return 0;
		}

		public void onContainersAllocated(List<Container> arg0) {
			// TODO Auto-generated method stub
			
		}

		public void onContainersCompleted(List<ContainerStatus> arg0) {
			// TODO Auto-generated method stub
			
		}

		public void onError(Throwable arg0) {
			// TODO Auto-generated method stub
			
		}

		public void onNodesUpdated(List<NodeReport> arg0) {
			// TODO Auto-generated method stub
			
		}

		public void onShutdownRequest() {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	private class AMNMCallbackHandler implements NMClientAsync.CallbackHandler {

		
		private ConcurrentMap<ContainerId,Container> containers;
		private final ApplicationMasterGA applicationMasterGA;
		
		public AMNMCallbackHandler(ApplicationMasterGA applicationMasterGA){
			this.applicationMasterGA = applicationMasterGA;
			containers = new ConcurrentHashMap<ContainerId, Container>();
		}
		
		public void addContainer(ContainerId containerId, Container container){
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
	public static void main(String[] args) {
		boolean result = false;
		ApplicationMasterGA appMaster = new ApplicationMasterGA();

		log.info("initialize application master");
		boolean doRun = appMaster.init(args);
		if (!doRun) {
			System.exit(0);
		}

		appMaster.run();
		result = appMaster.finish();

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
		
		conf = new YarnConfiguration();
		opts = new Options();
		opts.addOption("container_memory", true, "Amount of memory in MB for slave containers");
		opts.addOption("container_vcores", true, "No. of virtual cores for slave containers");
		opts.addOption("num_containers", true, "No of slave containers");
		opts.addOption("help", false, "Print usage");
	}

	public boolean init(String[] args) throws ParseException {
		CommandLine cliParser = new DefaultParser().parse(opts, args);

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
				+ ", clustertimestamp=" + appAttemptId.getApplicationId().getClusterTimestamp() 
				+ ", attemptId="
				+ appAttemptId.getAttemptId());
		
		
		containerMemory = Integer.parseInt(cliParser.getOptionValue(
				"container_memory","10"));
		containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores","1"));
		numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers","1"));
		
		if(numTotalContainers <= 0){
			throw new IllegalArgumentException(
					"Invalid number of container for running this app");
		}
		
		if(envs.get(Utils.USER_GA_JAR_HDFS_LOC).isEmpty()){
			throw new IllegalArgumentException(
					"No ga jar given in arguments.");
		}
		
		userGAJarHDFSLoc = envs.get(Utils.USER_GA_JAR_HDFS_LOC);
		
		if(envs.containsKey(Utils.USER_GA_JAR_HDFS_LEN)){
			userGAJarHDFSLen = Long.parseLong(envs.get(Utils.USER_GA_JAR_HDFS_LOC));
		}

		if(envs.containsKey(Utils.USER_GA_JAR_HDFS_TIMESTAMP)){
			userGAJarHDFSTimeStamp = Long.parseLong(envs.get(Utils.USER_GA_JAR_HDFS_TIMESTAMP));
		}
		
		
		if(userGAJarHDFSLoc.isEmpty() || userGAJarHDFSLen <=0 || userGAJarHDFSTimeStamp <= 0) {
			log.error("Illegal values in env for ga jar path =  "+userGAJarHDFSLoc+", len = "+userGAJarHDFSLen
					+", timestamp = "+userGAJarHDFSTimeStamp);
			throw new IllegalArgumentException("Illegal values of user ga jar loc, len "
					+ "and time stamp in env variable");
		}
		
		return true;
	}

	public void printUsage(Options opts) {
		new HelpFormatter().printHelp("ApplicationMaster", opts);
	}

	public boolean fileExist(String hdfsPath) {
		return true;
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

	public void run() throws IOException, YarnException{
		log.info("Starting application master");
		
		Credentials credentials = 
				UserGroupInformation.getCurrentUser().getCredentials();
		DataOutputBuffer dob = new DataOutputBuffer();
		
		credentials.writeTokenStorageToStream(dob);
		
		//remove am->rm token so that containers cannot access it.
		Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
		while(iter.hasNext()){
			Token<?> token = iter.next();
			log.info(token);
			if(token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)){
				iter.remove();
			}
		}
		
		allTokens = ByteBuffer.wrap(dob.getData(),0,dob.getLength());
		
		String appSubmitterUserName =
				System.getenv(ApplicationConstants.Environment.USER.$$());
		log.info("app submitter user name = "+appSubmitterUserName);
		appSubmitterUgi = 
				UserGroupInformation.createRemoteUser(appSubmitterUserName);
		appSubmitterUgi.addCredentials(credentials);
		
		
		
		AMRMClientAsync.CallbackHandler amRmAllocListener = 
				new AMRMCallbackHandler();
		amRmClient = AMRMClientAsync.createAMRMClientAsync(1000, amRmAllocListener);
		amRmClient.init(conf);
		amRmClient.start();
		
		amNmCallbackHandler = new AMNMCallbackHandler(this);
		amNmClientAsync = new NMClientAsyncImpl(amNmCallbackHandler);
		amNmClientAsync.init(conf);
		amNmClientAsync.start();
		
		startTimelineClient(conf);
		if(timelineClient!= null) {
			publishApplicationAttemptEvent(timelineClient,appAttemptId.toString(),
					DSEvent.DS_APP_ATTEMPT_START,appSubmitterUgi);
		}
		
		//register self with resource manager
		//this will start heartbeating to rm.
		
		appMasterHostname = NetUtils.getHostname();
		RegisterApplicationMasterResponse response = amRmClient.registerApplicationMaster(
				appMasterHostname,appMasterRpcPort,appMasterTrackingUrl);

		int maxMem = response.getMaximumResourceCapability().getMemory();
		log.info("max mem capability of containers node "+maxMem);
		
		int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
		log.info("max virtual cores of container nodes "+maxVCores);
		
		// A resource ask cannot exceed the max.
		if (containerMemory > maxMem) {
			log.info("Container memory specified above max threshold of cluster."
					+ " Using max value." + ", specified=" + containerMemory + ", max="
					+ maxMem);
			containerMemory = maxMem;
		}

		if (containerVirtualCores > maxVCores) {
			log.info("Container virtual cores specified above max threshold of cluster."
					+ " Using max value." + ", specified=" + containerVirtualCores + ", max="
					+ maxVCores);
			containerVirtualCores = maxVCores;
		}
		
		for(int i=0;i<numTotalContainers;i++){
			ContainerRequest containerAsk = setupContainerAskForRM();
			amRmClient.addContainerRequest(containerAsk);
		}
		numRequestedContainers.set(numTotalContainers);
		
	}
	


	public void startTimelineClient(final Configuration conf){
		appSubmitterUgi.doAs(new PrivilegedAction<Void>() {


			public Void run() {
				if(conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
						YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)){
					
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

	private static void publishApplicationAttemptEvent(
			final TimelineClient timelineClient, String appAttemptId,
			DSEvent appEvent,UserGroupInformation ugi){
		final TimelineEntity entity = new TimelineEntity();
		entity.setEntityId(appAttemptId);
		entity.setEntityType(DSEntity.DS_APP_ATTEMPT.toString());

		entity.addPrimaryFilter("user", ugi.getShortUserName());
		TimelineEvent event = new TimelineEvent();
		event.setEventType(appEvent.toString());
		event.setTimestamp(System.currentTimeMillis());
		entity.addEvent(event);
		
		try{
			timelineClient.putEntities(entity);
		} catch (YarnException e){
			log.error("App Attempt "
          + (appEvent.equals(DSEvent.DS_APP_ATTEMPT_START) ? "start" : "end")
          + " event could not be published for "
          + appAttemptId.toString(), e);
		} catch( IOException e) {
			log.error("App Attempt "
          + (appEvent.equals(DSEvent.DS_APP_ATTEMPT_START) ? "start" : "end")
          + " event could not be published for "
          + appAttemptId.toString(), e);
		}
		
	}
	
	private ContainerRequest setupContainerAskForRM(){
		Priority priority = Priority.newInstance(0);
		Resource capability = Resource.newInstance(containerMemory, containerVirtualCores);
		ContainerRequest request = new ContainerRequest(capability, null, null, priority);
		return request;
	}
}
