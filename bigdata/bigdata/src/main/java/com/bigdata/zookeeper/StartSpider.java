package com.bigdata.zookeeper;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class StartSpider {
    private IDownLoadService downLoadSerivce ;
    private IProcessService processService;
    private IRepositoryService repositoryService;

    public StartSpider() throws Exception {
        RetryPolicy retry = new ExponentialBackoffRetry(1000,3);
        CuratorFramework client= CuratorFrameworkFactory.builder()
                .connectString(ZKUtil.ZOOKEEPER_HOSTS)
                .connectionTimeoutMs(10000)
                .sessionTimeoutMs(10000)
                .retryPolicy(retry)
                .build();
        client.start();
        InetAddress localHost = InetAddress.getLocalHost();
        String ip = localHost.getHostAddress();
        client.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(ZKUtil.PATH+"/"+ip);
    }
    /**
     *爬虫应用执行入口
     */
    public static void main(String[] args) throws Exception {
        StartSpider ss = new StartSpider();
        ss.setDownLoadSerivce(new HttpClientDownLoadService());
        ss.setRepositoryService(new QueueRepositoryService());
        ss.setProcessService(new ProcessServiceImpl());
        //书籍
        String url ="http://www.lrts.me/book/category/3058";
        ss.repositoryService.addHighLevel(url);
        //开启爬虫
        ss.startSpider();
    }
    /**
     * 爬虫应用实现主逻辑
     */
    public void startSpider(){
        while(true){
            //数据仓库提取解析url
            final String url = repositoryService.poll();

            //判断url是否为空
            if(StringUtils.isNotBlank(url)){

            //下载
            Page page = StartSpider.this.downloadPage(url);

            //解析
            StartSpider.this.processPage(page);

            ThreadUtil.sleep((long) (Math.random() * 5000));
            }else{
                System.out.println("队列中的url解析完毕，请等待！");
                ThreadUtil.sleep((long) (Math.random() * 5000));
            }
        }
    }

    /**
     * 下载页面
     * @param url
     * @return
     */
    public Page downloadPage(String url){
        return this.downLoadSerivce.download(url);
    }

    /**
     * 页面解析
     * @param page
     */
    public void processPage(Page page){
        this.processService.process(page);
    }

    public IDownLoadService getDownLoadSerivce() {
        return downLoadSerivce;
    }

    public void setDownLoadSerivce(IDownLoadService downLoadSerivce) {
        this.downLoadSerivce = downLoadSerivce;
    }

    public IProcessService getProcessService() {
        return processService;
    }

    public void setProcessService(IProcessService processService) {
        this.processService = processService;
    }


    public IRepositoryService getRepositoryService() {
        return repositoryService;
    }

    public void setRepositoryService(IRepositoryService repositoryService) {
        this.repositoryService = repositoryService;
    }

}
