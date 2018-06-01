package com.weidu.ESClient.restHigh;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class SearchService {
	
	private static final Logger logger = LogManager.getLogger(SearchService.class);

	public RestHighLevelClient client;

	public SearchRequest searchRequest;

	public SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
	
	public SearchResponse searchResponse;
	
	public String index;
	
	public String type = "doc";

	public SearchService(Client client, String index) {
		this.client = client.getClient();
		this.index = index;
	}
	
	public SearchService(Client client, String index, String type) {
		this.client = client.getClient();
		this.index = index;
	}
	
	public List<Map<String, Object>> getSources() {
		List<Map<String, Object>> resutls = new ArrayList<>();
		searchRequest = new SearchRequest(index).types(type);
		searchRequest.source(searchSourceBuilder);
		try {
			searchResponse = client.search(searchRequest);
			SearchHits hits = searchResponse.getHits();
			logger.info("TotalHits:" + hits.getTotalHits());
			for (SearchHit hit : hits) {
				Map<String, Object> sourceAsMap = hit.getSourceAsMap();
				sourceAsMap.put("_id", hit.getId());
				resutls.add(sourceAsMap);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return resutls;
	}
	
	public SearchResponse getSearchResponse() {
		return searchResponse;
	}
	
	public SearchSourceBuilder getSearchSourceBuilder() {
		return searchSourceBuilder;
	}

	public void setSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder) {
		this.searchSourceBuilder = searchSourceBuilder;
	}

	

}
