package com.weidu.esclient.transport;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class Client {

	public TransportClient client;

	public Client(String ip, Integer port, String clusterName) {
		Settings settings = Settings.builder().put("cluster.name", clusterName).build();
		client = new PreBuiltTransportClient(settings);
		try {
			client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), port));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

	}

	public void close() {
		client.close();
	}

	public TransportClient getClient() {
		return client;
	}

}
