package com.weidu.esclient.transport;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.index.query.QueryBuilders;

public class Demo {

	public static void main(String[] args) {
		//index();
		//docs();
		query();
	}
	
	public static void query() {
		Client tranClient = new Client("192.168.1.75", 8300, "wdkj_test");
		SearchService tranQuery =  new SearchService(tranClient,"lhx_term");
		tranQuery.getSources(QueryBuilders.matchAllQuery());
	}
	
	public static void docs() {
		Client tranClient = new Client("192.168.1.75", 8300, "wdkj_test");
		DocService tranDS = new DocService(tranClient,"lhx_term");
		tranDS.index(Collections.singletonMap("org1", "test")).action();
		tranDS.index(Collections.singletonMap("org1", "test1")).add2RequestList();
		tranDS.index(Collections.singletonMap("org1", "test2")).add2RequestList();
		tranDS.index(Collections.singletonMap("org1", "test3")).add2RequestList();
		tranDS.bulkAction();
		System.out.println(tranDS.get("AWNjGozrPRmoooqGtP90"));
		tranDS.delete("AWNjHPwvPRmoooqGtP-G").action();
		tranClient.close();
	}
	
	public static void index() {
		Client tranClient = new Client("192.168.1.75", 8300, "wdkj_test");
		IndexService tranIndexService = new IndexService(tranClient);
		
		String mapping;
		try {
			String mappingPath = tranClient.getClass().getClassLoader().getResource("mapping_term.json").getPath();
			InputStream inputStream = new FileInputStream(mappingPath);
			mapping = IOUtils.toString(inputStream, "utf8");
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		
		tranIndexService.create("lhx_term", mapping);
		//tranIndexService.delete("lhx_term");
		tranClient.close();
	}
	
	
}
