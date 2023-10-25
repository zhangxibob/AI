package com.bigdata.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.ArrayList;
import java.util.List;
/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class SpiderWatcher implements Watcher {
    public CuratorFramework client;
    List<String> oldList = new ArrayList<>();
    public SpiderWatcher(){
        RetryPolicy retry = new ExponentialBackoffRetry(1000,3);
        client= CuratorFrameworkFactory.builder()
                .connectString(ZKUtil.ZOOKEEPER_HOSTS)
                .connectionTimeoutMs(10000)
                .sessionTimeoutMs(10000)
                .retryPolicy(retry)
                .build();
        client.start();
        try {
            oldList=client.getChildren().usingWatcher(this).forPath(ZKUtil.PATH);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public void process(WatchedEvent event) {
        if(event.getType()==Event.EventType.NodeChildrenChanged){
            try {
                displayChildren();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void displayChildren() throws Exception {
        List<String> currentList = client.getChildren().usingWatcher(this).forPath(ZKUtil.PATH);
        for (String child:currentList){
            if(!oldList.contains(child)){
                System.out.println("爬虫应用："+child+"刚刚上线！");
            }
        }

        for (String child:oldList){
            if(!currentList.contains(child)){
                System.out.println("爬虫应用："+child+"刚刚下线！");
            }
        }
        this.oldList=currentList;
    }

    public static void main(String[] args) throws InterruptedException {
        SpiderWatcher spiderWatcher = new SpiderWatcher();
        Thread.sleep(Long.MAX_VALUE);
    }
}
