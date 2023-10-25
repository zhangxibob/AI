package com.bigdata.zookeeper;
/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class HttpClientDownLoadService implements IDownLoadService {

	public Page download(String url) {
		// TODO Auto-generated method stub
		Page page = new Page();
		page.setContent(PageDownLoadUtil.getPageContent(url));
		page.setUrl(url);
		return page;
	}

}
