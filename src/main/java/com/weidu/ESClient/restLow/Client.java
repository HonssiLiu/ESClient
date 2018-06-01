package com.weidu.ESClient.restLow;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

public class Client {

	private RestClient client;

	public Client() {
		client = RestClient.builder(new HttpHost("localhost", 9200, "http")).setMaxRetryTimeoutMillis(10 * 60 * 1000).build();
	}

	public Client(String ip, Integer port) {
		client = RestClient.builder(new HttpHost(ip, port, "http")).setMaxRetryTimeoutMillis(10 * 60 * 1000).build();
	}

	public void close() {
		try {
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public RestClient getClient() {
		return client;
	}

}
