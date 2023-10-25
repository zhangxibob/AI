package com.bigdata.zookeeper;
/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public interface IRepositoryService {

	public String poll();
	
	public void addHighLevel(String url);
	
	public void addLowLevel(String url);
}
