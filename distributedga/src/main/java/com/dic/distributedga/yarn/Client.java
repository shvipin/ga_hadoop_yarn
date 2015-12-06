package com.dic.distributedga.yarn;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class Client {
	private static final Log log = LogFactory.getLog(Client.class);
	private static final String appMasterJarHDFSPath = "AppMaster.jar";

	private Options opts;
	private boolean debugFlag;
	private String appName;
	private int amMemory;
	private int amVCores;
	private String amJarPath;
	private int containerMemory;
	private int containerVirtualCores;
	private int numContainers;

	private Configuration conf;
	private String amMainClass;
	private YarnClient yarnClient;

	public static void main(String[] args) {
		boolean result = false;
		Client client = new Client();

		try {

			client.init(args);
			result = client.run();

		} catch (ParseException e) {
			e.printStackTrace();
			log.fatal("Error running client");
			System.exit(2);
		} catch (IllegalArgumentException e) {
			e.getLocalizedMessage();
			client.printUsage();
			System.exit(2);
		}

		if (result) {
			log.info("Application completed successfully");
			System.exit(0);
		}

		log.info("Applicatoin failed to complete successfully");
		System.exit(1);

	}

	public Client() {
		this(new YarnConfiguration());
	}

	public Client(Configuration config) {
		this("com.dic.distributedga.yarn.GAMaster", config);
	}

	public Client(String amMainClass, Configuration config) {

		this.conf = config;
		this.amMainClass = amMainClass;
		yarnClient = YarnClient.createYarnClient();

		opts = new Options();
		opts.addOption("appname", true, "Application name. Default value - DistributedGA");
		opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
		opts.addOption("master_vcores", true, "Amount of virtual cores to be requested to run the application master");
		opts.addOption("jar", true, "Jar file containing the application master");
		opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command");
		opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run the shell command");
		opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed");
		opts.addOption("debug", false, "Dump out debug information");
		opts.addOption("help", false, "Print usage");
	}

	public boolean init(String[] args) throws ParseException {
		log.info("Initializing client");
		CommandLine cliParser = new DefaultParser().parse(opts, args);

		if (args.length == 0) {
			throw new IllegalArgumentException("No args specified for client to initialize");
		}

		if (cliParser.hasOption("help")) {
			printUsage();
			return false;
		}

		if (!cliParser.hasOption("jar")) {
			throw new IllegalArgumentException("No jar file specified for application master");
		}

		debugFlag = Boolean.parseBoolean(cliParser.getOptionValue("debug", "false"));
		appName = cliParser.getOptionValue("appname", "DistributedGA");
		amJarPath = cliParser.getOptionValue("jar");
		amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "10"));
		amVCores = Integer.parseInt(cliParser.getOptionValue("master_vcores", "1"));

		if (amMemory <= 0) {
			throw new IllegalArgumentException(
					"Invalid memory specified for application master, exiting." + " Specified memory=" + amMemory);
		}
		if (amVCores <= 0) {
			throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
					+ " Specified virtual cores=" + amVCores);
		}

		containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
		containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
		numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));

		if (containerMemory < 0 || containerVirtualCores < 0 || numContainers < 1) {
			throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified,"
					+ " exiting." + " Specified containerMemory=" + containerMemory + ", containerVirtualCores="
					+ containerVirtualCores + ", numContainer=" + numContainers);
		}

		return true;
	}

	private void printUsage() {
		new HelpFormatter().printHelp("Client", opts);
	}

	public boolean run() throws YarnException, IOException {
		log.info("Running client");
		yarnClient.start();

		YarnClientApplication app = yarnClient.createApplication();
		GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

		int maxMem = appResponse.getMaximumResourceCapability().getMemory();
		log.info("Max mem capabililty of resources in this cluster " + maxMem);

		// A resource ask cannot exceed the max.
		if (amMemory > maxMem) {
			log.info("AM memory specified above max threshold of cluster. Using max value." + ", specified=" + amMemory
					+ ", max=" + maxMem);
			amMemory = maxMem;
		}

		int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
		log.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);

		if (amVCores > maxVCores) {
			log.info("AM virtual cores specified above max threshold of cluster. " + "Using max value." + ", specified="
					+ amVCores + ", max=" + maxVCores);
			amVCores = maxVCores;
		}

		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		ApplicationId appId = appContext.getApplicationId();

		appContext.setApplicationName(appName);

		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		FileSystem fs = FileSystem.get(conf);
		
		log.info("Copy app master jar from local file system and add to local environment");
		addToLocalResources(fs, amJarPath, appMasterJarHDFSPath, appId.toString(), localResources, null);

		
		return true;
	}

	public void addToLocalResources(FileSystem fs, String fileSrcPath, String fileDstPath, String appId,
			Map<String, LocalResource> localResources, String resources) throws IOException {
		String suffix = appName + "/" + "appId" + "/" + fileDstPath;
		Path dst = new Path(fs.getHomeDirectory(), suffix);

		if (fileSrcPath == null) {
			FSDataOutputStream ostream = null;

			try {
				ostream = FileSystem.create(fs, dst, new FsPermission((short) 0710));
				ostream.writeUTF(resources);
			} finally {
				IOUtils.closeQuietly(ostream);
			}

		}
		else {
			fs.copyFromLocalFile(new Path(fileSrcPath), dst);
		}
		
		FileStatus scFileStatus = fs.getFileStatus(dst);
		LocalResource scRsrc = LocalResource.newInstance(
				ConverterUtils.getYarnUrlFromURI(dst.toUri()),
				LocalResourceType.FILE,
				LocalResourceVisibility.APPLICATION,
				scFileStatus.getLen(),
				scFileStatus.getModificationTime());
		localResources.put(fileDstPath, scRsrc);
	}
}
