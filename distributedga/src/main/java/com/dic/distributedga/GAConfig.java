package com.dic.distributedga;

public class GAConfig {

	public static final String APP_NAME = "distributedga";
	public static final int  MASTER_MEMORY = 10;
	public static final int  MASTER_V_CORES = 1;
	public static final int  CONTAINER_MEMORY = 10;
	public static final int  CONTAINER_V_CORES = 1;

	private String appName;
	private int masterMemory;
	private int masterVCores;
	private String jarPath;
	private int containersMemory;
	private int containersVCores;
	private int containersCount;
	/**
	 * @return the appName
	 */
	public String getAppName() {
		if(appName == null){
			return APP_NAME;
		}
		return appName;
	}
	/**
	 * @param appName the appName to set
	 */
	public void setAppName(String appName) {
		this.appName = appName;
	}
	/**
	 * @return the masterMemory
	 */
	public int getMasterMemory() {
		if(masterMemory == 0){
			return MASTER_MEMORY;
		}
		return masterMemory;
	}
	/**
	 * @param masterMemory the masterMemory to set
	 */
	public void setMasterMemory(int masterMemory) {
		this.masterMemory = masterMemory;
	}
	/**
	 * @return the masterVCores
	 */
	public int getMasterVCores() {
		if(masterVCores == 0){
			return MASTER_V_CORES;
		}
		return masterVCores;
	}
	/**
	 * @param masterVCores the masterVCores to set
	 */
	public void setMasterVCores(int masterVCores) {
		this.masterVCores = masterVCores;
	}
	/**
	 * @return the jarPath
	 */
	public String getJarPath() {
		return jarPath;
	}
	/**
	 * @param jarPath the jarPath to set
	 */
	public void setJarPath(String jarPath) {
		this.jarPath = jarPath;
	}
	/**
	 * @return the containersMemory
	 */
	public int getContainersMemory() {
		if(containersMemory == 0){
			return CONTAINER_MEMORY;
		}
		return containersMemory;
	}
	/**
	 * @param containersMemory the containersMemory to set
	 */
	public void setContainersMemory(int containersMemory) {
		this.containersMemory = containersMemory;
	}
	/**
	 * @return the containersVCores
	 */
	public int getContainersVCores() {
		if(containersVCores == 0){
			return CONTAINER_V_CORES;
		}
		return containersVCores;
	}
	/**
	 * @param containersVCores the containersVCores to set
	 */
	public void setContainersVCores(int containersVCores) {
		this.containersVCores = containersVCores;
	}
	/**
	 * @return the containersCount
	 */
	public int getContainersCount() {
		
		return containersCount;
	}
	/**
	 * @param containersCount the containersCount to set
	 */
	public void setContainersCount(int containersCount) {
		this.containersCount = containersCount;
	}
	
}
