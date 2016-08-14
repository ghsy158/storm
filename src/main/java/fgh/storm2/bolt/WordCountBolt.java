package fgh.storm2.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 计数
 * 
 * @author fgh
 * @since 2016年8月14日下午4:20:58
 */
public class WordCountBolt implements IRichBolt {
	private static final long serialVersionUID = 2596040869814442638L;

	private OutputCollector collector;
	private HashMap<String, Long> counts = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.counts = new HashMap<String ,Long>();
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Long count = counts.get(word);
		if(count==null){
			count = 0L;
		}
		count ++;
		this.counts.put(word, count);
		this.collector.emit(new Values(word,count));
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
