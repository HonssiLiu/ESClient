package com.weidu.ESClient.restHigh;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;

public class SearchScrollService extends SearchService implements Iterator<Map<String, Object>> {

	private static final Logger logger = LogManager.getLogger(SearchScrollService.class);

	private Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));

	private String scrollId;

	private SearchHit[] searchHits;

	private int it = 0;

	public SearchScrollService(Client client, String index) {
		super(client, index);
	}

	public SearchScrollService(Client client, String index, String type) {
		super(client, index, type);
	}

	public void init() {
		searchRequest = new SearchRequest(index).types(type);
		searchRequest.scroll(scroll);
		searchRequest.source(searchSourceBuilder);
		//System.out.println(searchSourceBuilder.toString());
		try {
			SearchResponse searchResponse = client.search(searchRequest);
			logger.info("TotalHits:" + searchResponse.getHits().getTotalHits());
			scrollId = searchResponse.getScrollId();
			searchHits = searchResponse.getHits().getHits();
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	private boolean scroll() {
		if (searchHits != null && searchHits.length > 0) {
			SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
			scrollRequest.scroll(scroll);
			try {
				SearchResponse searchResponse = client.searchScroll(scrollRequest);
				scrollId = searchResponse.getScrollId();
				searchHits = searchResponse.getHits().getHits();
				it = 0;
				return true;
			} catch (IOException e) {
				logger.error(e.getMessage());
				return false;
			}
		} else {
			return false;
		}
	}

	@Override
	public boolean hasNext() {
		if (searchHits == null || searchHits.length == 0) {
			return false;
		} else if (it < searchHits.length) {
			return true;
		} else {
			scroll();
			return hasNext();
		}
	}

	@Override
	public Map<String, Object> next() {
		SearchHit hit = searchHits[it++];
		Map<String, Object> sourceAsMap = hit.getSourceAsMap();
		sourceAsMap.put("_id", hit.getId());
		return sourceAsMap;
	}

	public void clean() {
		ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
		clearScrollRequest.addScrollId(scrollId);
		try {
			client.clearScroll(clearScrollRequest);
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

}
