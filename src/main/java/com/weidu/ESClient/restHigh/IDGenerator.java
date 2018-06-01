package com.weidu.ESClient.restHigh;

import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * 生成ID
 * @author Administrator
 *
 */
public class IDGenerator {
	
	public static synchronized String newId(){
		return DigestUtils.md5Hex(UUID.randomUUID().toString());
	}
	
	public static synchronized String newId(String str){
		return DigestUtils.md5Hex(str);
	}

}
