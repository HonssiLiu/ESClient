package com.weidu.ESClient.transport;

import java.util.Iterator;
import java.util.Map;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.alibaba.fastjson.JSON;

public class Demo {

	public static void main(String[] args) {
		Client client = new Client();
		SearchService tranQuery = new SearchService(client, "orgtag");
		QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
		Iterator<Map<String, Object>> ite = tranQuery.getSourceIterator(queryBuilder);
		while (ite.hasNext()) {
			Map<String, Object> ss = ite.next();
			String jsonstr = JSON.toJSONString(ss, false);
			System.out.println(jsonstr);
		}
	}

}
