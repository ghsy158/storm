package fgh.storm2.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 拆分
 * 
 * @author fgh
 * @since 2016年8月14日下午4:17:02
 */
public class WordSplitBolt implements IRichBolt {

	private static final long serialVersionUID = 8804420139171772449L;

	private static final String ERROR_STR = "don't have a cow man";

	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String sentcence = input.getStringByField("sentence");
		String[] words = sentcence.split(" ");
		for (String word : words) {
			this.collector.emit(new Values(word));
		}
	}

	@Override
	public void cleanup() {
		System.out.println("------------WordSplitBolt cleanup------------");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
