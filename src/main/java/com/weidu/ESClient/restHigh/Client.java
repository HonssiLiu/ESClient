package com.weidu.ESClient.restHigh;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class Client {

	private RestHighLevelClient client;
	
	public Client() {
		client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")).setMaxRetryTimeoutMillis(10 * 60 * 1000));
	}

	public Client(String ip, Integer port) {
		client = new RestHighLevelClient(RestClient.builder(new HttpHost(ip, port, "http")).setMaxRetryTimeoutMillis(10 * 60 * 1000));
	}

	public void close() {
		try {
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public RestHighLevelClient getClient() {
		return client;
	}

}
