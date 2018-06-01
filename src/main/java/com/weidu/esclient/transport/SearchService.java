package com.weidu.esclient.transport;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class SearchService {

	public TransportClient client;

	public String index;

	public String[] types;

	public long totalHits;

	public SearchService(Client client, String index, String... types) {
		this.client = client.getClient();
		this.index = index;
		this.types = types;
	}

	public List<Map<String, Object>> getSources(QueryBuilder queryBuilder, int... size) {
		List<Map<String, Object>> results = new ArrayList<>();
		SearchRequestBuilder builder = client.prepareSearch(index);
		if (size.length > 0) {
			builder.setSize(size[0]);
		}
		builder.setQuery(queryBuilder);
		if (types.length > 0)
			builder.setTypes(types);
		// System.out.println(builder.toString());
		SearchResponse searchResponse = builder.execute().actionGet();
		totalHits = searchResponse.getHits().getTotalHits();
		System.out.println("共查询出%d条数据" + totalHits);
		SearchHits hits = searchResponse.getHits();
		for (SearchHit hit : hits) {
			Map<String, Object> sourceAsMap = hit.getSourceAsMap();
			sourceAsMap.put("_id", hit.getId());
			results.add(sourceAsMap);
		}
		return results;
	}

	public List<String> getSourcesAsString(QueryBuilder queryBuilder, int... size) {
		List<String> results = new ArrayList<>();
		SearchRequestBuilder builder = client.prepareSearch(index);
		if (size.length > 0) {
			builder.setSize(size[0]);
		}
		builder.setQuery(queryBuilder);
		if (types.length > 0)
			builder.setTypes(types);
		// System.out.println(builder.toString());
		SearchResponse searchResponse = builder.execute().actionGet();
		totalHits = searchResponse.getHits().getTotalHits();
		System.out.println("共查询出%d条数据" + totalHits);
		SearchHits hits = searchResponse.getHits();
		for (SearchHit hit : hits) {
			String sourceAsString = hit.getSourceAsString();
			results.add(sourceAsString);
		}
		return results;
	}

	public Iterator<Map<String, Object>> getSourceIterator(QueryBuilder queryBuilder, int... size) {
		SearchRequestBuilder builder = client.prepareSearch(index);
		if (size.length > 0) {
			builder.setSize(size[0]);
		}
		builder.setQuery(queryBuilder);
		if (types.length > 0)
			builder.setTypes(types);
		builder.setScroll(TimeValue.timeValueMinutes(60));
		// System.out.println(builder.toString());
		SearchResponse searchResponse = builder.get();
		totalHits = searchResponse.getHits().getTotalHits();
		System.out.println("共查询出%d条数据" + totalHits);
		return new BulkInterator(searchResponse);
	}

	public class BulkInterator implements Iterator<Map<String, Object>> {

		private SearchHit[] hits;

		private SearchResponse searchResponse;

		private int count = 0;

		private int index = 0;

		public BulkInterator(SearchResponse searchResponse) {
			this.searchResponse = searchResponse;
			count = searchResponse.getHits().getHits().length;
			if (count > 0) {
				hits = searchResponse.getHits().getHits();
				index = 0;
			}
		}

		@Override
		public boolean hasNext() {
			if (count == index) {
				searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(60)).execute().actionGet();
				count = searchResponse.getHits().getHits().length;
				if (count > 0) {// 还有数据
					hits = searchResponse.getHits().getHits();
					index = 0;
					return true;
				}
			} else if (index < count) {
				return true;
			}
			return false;
		}

		@Override
		public Map<String, Object> next() {
			SearchHit hit = hits[index++];
			Map<String, Object> sourceAsMap = hit.getSourceAsMap();
			sourceAsMap.put("_id", hit.getId());
			return sourceAsMap;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

}
