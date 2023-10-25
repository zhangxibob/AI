package com.bigdata.zookeeper;

/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class ThreadUtil {
	public static void sleep(long millions){
		try {
			Thread.currentThread().sleep(millions);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		while(true){
			System.out.println((long)(Math.random() *5000));
		}
		
	}
}
