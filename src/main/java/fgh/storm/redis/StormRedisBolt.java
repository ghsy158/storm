package fgh.storm.redis;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

/**
 * 
 * @author fgh
 * @since 2016年8月28日上午10:47:15
 */
public class StormRedisBolt implements IRichBolt {

	private static final long serialVersionUID = 6340129730067083801L;

	private RedisOperation redisOperation = null;
	
	private String redisIp = null;
	
	private int port;
	
	public StormRedisBolt(String redisIp, int port) {
		this.redisIp = redisIp;
		this.port = port;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		redisOperation  = new RedisOperation(this.redisIp,this.port);
	}

	@Override
	public void execute(Tuple input) {
		Map<String,Object> record = new HashMap<String,Object>();
		record.put("firstName", input.getStringByField("firstName"));
		record.put("lastName", input.getStringByField("lastName"));
		record.put("companyName", input.getStringByField("companyName"));
		
		redisOperation.insert(record,UUID.randomUUID().toString());
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
