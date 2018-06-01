package com.weidu.ESClient.transport;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class SearchService {

	public TransportClient client;

	public String index;

	public String[] types;

	public SearchService(Client client, String index, String... types) {
		this.client = client.getClient();
		this.index = index;
		this.types = types;
	}

	public List<Map<String, Object>> getSources(QueryBuilder queryBuilder) {
		List<Map<String, Object>> results = new ArrayList<>();
		SearchRequestBuilder builder = client.prepareSearch(index).setSize(100);
		builder.setQuery(queryBuilder);
		if (types.length > 0)
			builder.setTypes(types);
		//System.out.println(builder.toString());
		SearchResponse searchResponse = builder.execute().actionGet();
		System.out.println("共查询出%d条数据" + searchResponse.getHits().getTotalHits());
		SearchHits hits = searchResponse.getHits();
		for (SearchHit hit : hits) {
			Map<String, Object> sourceAsMap = hit.getSourceAsMap();
			sourceAsMap.put("_id", hit.getId());
			results.add(sourceAsMap);
		}
		return results;
	}

	public Iterator<Map<String, Object>> getSourceIterator(QueryBuilder queryBuilder) {
		SearchRequestBuilder builder = client.prepareSearch(index).setSize(100);
		builder.setQuery(queryBuilder);
		if (types.length > 0)
			builder.setTypes(types);
		builder.setSearchType(SearchType.DEFAULT).setScroll(TimeValue.timeValueMinutes(60));
		//System.out.println(builder.toString());
		SearchResponse searchResponse = builder.execute().actionGet();
		System.out.println("共查询出%d条数据" + searchResponse.getHits().getTotalHits());
		return new BulkInterator(searchResponse.getScrollId());
	}

	public class BulkInterator implements Iterator<Map<String, Object>> {

		private SearchHit[] hits;

		private String scrollId;

		private int count = 0;

		private int index = 0;

		public BulkInterator(String scrollId) {
			this.scrollId = scrollId;
		}

		@Override
		public boolean hasNext() {
			if (count == index) {
				SearchResponse searchResponse = client.prepareSearchScroll(scrollId).setScroll(TimeValue.timeValueMinutes(60)).execute().actionGet();
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
