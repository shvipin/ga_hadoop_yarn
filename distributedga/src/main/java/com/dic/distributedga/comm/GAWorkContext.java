package com.dic.distributedga.comm;

import java.io.Serializable;

import com.dic.distributedga.core.abstractga.BasePopulation;

public class GAWorkContext implements Serializable {
	
	BasePopulation wPopulation;	
	/*Solution criteria*/
	String wSolution;
	/*Other genetic algorithm context paramters..  
	 * 
	 */
	int flag;
	
	public void setFlag(int flag){
		this.flag = flag;
	}
	
	public int getFlag(){
		return flag;
	}
	
	public BasePopulation getPopulation() {
		return wPopulation;
	}

	public void setPopulation(BasePopulation wPopulation) {
		this.wPopulation = wPopulation;
	}

	public String getSolution() {
		return wSolution;
	}

	public void setSolution(String wSolution) {
		this.wSolution = wSolution;
	}
	
}
