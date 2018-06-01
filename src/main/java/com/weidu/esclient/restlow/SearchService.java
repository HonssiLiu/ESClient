package com.weidu.esclient.restlow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class SearchService {

	public RestClient client;

	public String index;

	public String type = "doc";

	public SearchService(Client client, String index) {
		this.client = client.getClient();
		this.index = index;
	}

	public static void main(String[] args) {
		String querystring = "{\"query\":{\"match_all\":{}},\"size\":20}";
		Client client = new Client();
		SearchService ss = new SearchService(client, "orgtag");
		List<JSONObject> jsons = ss.getSource(querystring);
		System.out.println(JSONObject.toJSONString(jsons, false));
		client.close();
	}

	public List<JSONObject> getSource(String querystring) {
		try {
			Map<String, String> params = Collections.emptyMap();
			HttpEntity entity = new NStringEntity(querystring, ContentType.APPLICATION_JSON);
			String endpoint = "/" + index + "/" + type + "/_search";
			Response response = client.performRequest("GET", endpoint, params, entity);
			String resString = IOUtils.toString(response.getEntity().getContent(), "utf-8");
			List<JSONObject> list = unmarsh(resString);
			return list;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public JSONObject getRes(String querystring) {
		try {
			Map<String, String> params = Collections.emptyMap();
			HttpEntity entity = new NStringEntity(querystring, ContentType.APPLICATION_JSON);
			String endpoint = "/" + index + "/" + type + "/_search";
			Response response = client.performRequest("GET", endpoint, params, entity);
			String resString = IOUtils.toString(response.getEntity().getContent(), "utf-8");
			return JSONObject.parseObject(resString);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public List<JSONObject> unmarsh(String resString) throws Exception {
		JSONArray array = JSONObject.parseObject(resString).getJSONObject("hits").getJSONArray("hits");
		if (array != null && array.size() > 0) {
			List<JSONObject> list = new ArrayList<>();
			for (int i = 0; i < array.size(); i++) {
				JSONObject source = array.getJSONObject(i).getJSONObject("_source");
				if (source != null) {
					list.add(source);
				}
			}
			if (list.size() > 0)
				return list;
		}
		return null;
	}

}
