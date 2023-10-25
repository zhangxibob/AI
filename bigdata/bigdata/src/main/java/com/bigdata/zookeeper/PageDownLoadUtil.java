package com.bigdata.zookeeper;

import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class PageDownLoadUtil {
	private final static String USER_AGENT="Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36";
	
	public static String getPageContent(String url){
		HttpClientBuilder builder = HttpClients.custom();
		CloseableHttpClient client = builder.build();
		
		HttpGet request = new HttpGet(url);
		String content = null;
		try {
			request.setHeader("User-Agent",USER_AGENT);
			CloseableHttpResponse response = client.execute(request);
			HttpEntity  entity = response.getEntity();
			content = EntityUtils.toString(entity);
			//System.out.println(content);
		} catch(HttpHostConnectException e){
			e.printStackTrace();
		}catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return content;	
	}
}
