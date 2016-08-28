package fgh.storm.redis;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import redis.clients.jedis.Jedis;

/**
 * 
 * @author fgh
 * @since 2016年8月28日上午10:46:24
 */
public class RedisOperation implements Serializable {

	private static final long serialVersionUID = 1880675395757599926L;

	Jedis jedis = null;

	public RedisOperation(String redisIp, int port) {
		jedis = new Jedis(redisIp, port);
	}

	public void insert(Map<String, Object> record, String id) {
		try {
			System.out.println("保存到redis,id【"+id+"】");
			jedis.set(id, new ObjectMapper().writeValueAsString(record));
		} catch (IOException e) {
			System.out.println("record not persist into datastore...");
		}
	}

}
