package com.weidu.ESClient.transport;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class Client {

	public TransportClient client;

	@SuppressWarnings("resource")
	public Client(String ip, String cluster) {
		try {
			Settings settings = Settings.builder().put("cluster.name", cluster).build();
			client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName(ip), 9300));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("resource")
	public Client() {
		try {
			client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
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
