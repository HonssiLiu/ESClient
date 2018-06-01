package com.weidu.ESClient.restHigh;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

public class DocService {

	private static final Logger logger = LogManager.getLogger(DocService.class);

	public RestHighLevelClient client;

	public String index;

	public String type = "doc";

	public boolean upsert = false;

	private DocWriteRequest<?> request;

	private DocWriteResponse response;

	private List<DocWriteRequest<?>> requestList = new ArrayList<>();

	private int bulkSize = 1000;

	public DocService(Client client, String index) {
		this.client = client.getClient();
		this.index = index;
	}

	public DocService action() {
		try {
			if (request instanceof IndexRequest) {
				response = client.index((IndexRequest) request);
			} else if (request instanceof DeleteRequest) {
				response = client.delete((DeleteRequest) request);
			} else if (request instanceof UpdateRequest) {
				response = client.update((UpdateRequest) request);
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		docWriteResponseHandler();
		request = null;
		return this;
	}

	public DocService index(String id, String jsonString) {
		request = new IndexRequest(index, type, id).source(jsonString, XContentType.JSON);
		return this;
	}

	public DocService index(String id, Map<String, Object> jsonMap) {
		request = new IndexRequest(index, type, id).source(jsonMap);
		return this;
	}

	public DocService index(String jsonString) {
		request = new IndexRequest(index, type).source(jsonString, XContentType.JSON);
		return this;
	}

	public DocService index(Map<String, Object> jsonMap) {
		request = new IndexRequest(index, type).source(jsonMap);
		return this;
	}

	public DocService delete(String id) {
		request = new DeleteRequest(index, type, id);
		return this;
	}

	public DocService update(String id, String jsonString) {
		request = new UpdateRequest(index, type, id).doc(jsonString, XContentType.JSON).docAsUpsert(upsert);
		return this;
	}

	public DocService update(String id, Map<String, Object> jsonMap) {
		request = new UpdateRequest(index, type, id).doc(jsonMap).docAsUpsert(upsert);
		return this;
	}

	public DocService add2RequestList() {
		if (request != null)
			requestList.add(request);
		if (requestList.size() >= bulkSize) {
			bulkAction();
		}
		return this;
	}

	public DocService bulkAction() {
		if (requestList.size() == 0) {
			logger.info("requestList is empty!");
			return this;
		}
		BulkRequest bulkRequest = new BulkRequest();
		for (DocWriteRequest<?> docWriteRequest : requestList) {
			bulkRequest.add(docWriteRequest);
		}
		try {
			BulkResponse bulkResponse = client.bulk(bulkRequest);
			if (bulkResponse.hasFailures()) {
				logger.error("BulkResponse has Failures! Retrieving the failure information...");
				for (BulkItemResponse bulkItemResponse : bulkResponse) {
					if (bulkItemResponse.isFailed()) {
						BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
						logger.error(failure.getIndex() + "/" + failure.getType() + "/" + failure.getId() + "/ is Failed, Because...");
						logger.error(failure.getCause().getMessage());
					}
				}
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		requestList.clear();
		request = null;
		return this;
	}

	/**
	 * 
	 * @param index
	 * @param type
	 * @param id
	 * @param includes:
	 *            String[] includes = new String[]{"message", "*Date"}
	 */
	public Map<String, Object> get(String id, String... includes) {
		GetRequest request = new GetRequest(index, type, id);
		if (includes.length > 0) {
			FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, Strings.EMPTY_ARRAY);
			request.fetchSourceContext(fetchSourceContext);
		}
		try {
			GetResponse getResponse = client.get(request);
			if (getResponse.isExists()) {
				Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
				return sourceAsMap;
			} else {
				//logger.error(index + "/" + type + "/" + id + "/doc was not found!");
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}

	public void docWriteResponseHandler() {
		String index = response.getIndex();
		String type = response.getType();
		String id = response.getId();
		long version = response.getVersion();
		String info = index + "/" + type + "/" + id + "/" + version + "/";
		if (response.getResult() == DocWriteResponse.Result.CREATED) {
			logger.info(info + "doc created.");
		} else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
			logger.info(info + "doc updated.");
		} else if (response.getResult() == DocWriteResponse.Result.DELETED) {
			logger.info(info + "doc deleted.");
		} else if (response.getResult() == DocWriteResponse.Result.NOOP) {
			logger.info(info + "doc no operation.");
		} else if (response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
			logger.error(info + "doc was not found!");
		}
		ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
		// if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
		// logger.info(info + "number of successful shards is less than total shards!");
		// }
		if (shardInfo.getFailed() > 0) {
			for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
				logger.error(info + failure.reason());
			}
		}
		response = null;
	}

	public void setBulkSize(int bulkSize) {
		this.bulkSize = bulkSize;
	}

}
