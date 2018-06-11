package com.weidu.esclient.transport;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

public class IndexService {

    private TransportClient client;

    private String type = "doc";

    private long max_result_window = 10000000;

    public IndexService(Client client) {
        this.client = client.getClient();
    }

    public void create(String index, String jsonStingMapping, Integer... shards) {
        CreateIndexRequest request = new CreateIndexRequest(index);
        if (shards != null && shards.length > 1)
            request.settings(Settings.builder().put("index.number_of_shards", shards[0]).put("index.number_of_replicas", shards[1]));
        if (jsonStingMapping != null)
            request.mapping(type, jsonStingMapping, XContentType.JSON);
        request.settings(Settings.builder().put("max_result_window", max_result_window));
        client.admin().indices().create(request).actionGet();
    }

    public void delete(String index) {
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        client.admin().indices().delete(request).actionGet();
    }

    public boolean exist(String index) {
        IndicesExistsRequest request = new IndicesExistsRequest(index);
        IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
        return response.isExists();
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setMax_result_window(long max_result_window) {
        this.max_result_window = max_result_window;
    }


}
