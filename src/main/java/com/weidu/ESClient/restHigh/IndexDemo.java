package com.weidu.ESClient.restHigh;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

public class IndexDemo {

	public static void main(String[] args) throws IOException {
		Client client = new Client();
		IndexService indexservice = new IndexService(client);
		String mappingPath = client.getClass().getClassLoader().getResource("mapping.json").getPath();
		InputStream inputStream = new FileInputStream(mappingPath);
		String mapping = IOUtils.toString(inputStream, "utf8");
		//indexservice.deleteIndex("postcode_count");
		indexservice.createIndex("postcode_count", mapping);
		client.close();
	}

}
