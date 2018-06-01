package com.weidu.ESClient.restHigh;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

public class IndexService {

	public RestHighLevelClient client;
	
	public String type = "doc";

	public IndexService(Client client) {
		this.client = client.getClient();
	}
	
	public void creatIndex(String index, String mappingfile) {
		String mappingPath = client.getClass().getClassLoader().getResource(mappingfile).getPath();
		String mapping;
		try {
			InputStream inputStream = new FileInputStream(mappingPath);
			mapping = IOUtils.toString(inputStream, "utf8");
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		try {
			deleteIndex(index);
		} catch (Exception e) {
			e.printStackTrace();
		}
		createIndex(index, mapping);
	}

	public CreateIndexResponse createIndex(String index, String jsonStingMapping, Integer... shards) {
		CreateIndexRequest request = new CreateIndexRequest(index);
		if (shards != null && shards.length > 1)
			request.settings(Settings.builder().put("index.number_of_shards", shards[0]).put("index.number_of_replicas",
					shards[1]));
		if (jsonStingMapping != null)
			request.mapping(type, jsonStingMapping, XContentType.JSON);
		request.settings(Settings.builder().put("max_result_window", 10000000));
		try {
			return client.indices().create(request);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public DeleteIndexResponse deleteIndex(String index) {
		DeleteIndexRequest request = new DeleteIndexRequest(index);
		try {
			return client.indices().delete(request);
		} catch (ElasticsearchException | IOException exception) {
			exception.printStackTrace();
		}
		return null;
	}

	public OpenIndexResponse openIndex(String index) {
		OpenIndexRequest request = new OpenIndexRequest(index);
		try {
			return client.indices().open(request);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public CloseIndexResponse closeIndex(String index) {
		CloseIndexRequest request = new CloseIndexRequest(index);
		try {
			return client.indices().close(request);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

}
