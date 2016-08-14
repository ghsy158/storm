package fgh.storm2.spout;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import fgh.util.Utils;

/**
 * 
 * @author fgh
 * @since 2016年8月14日下午4:03:11
 */
public class WordSpout implements IRichSpout {

	private static final long serialVersionUID = 5691484307591805676L;

	private SpoutOutputCollector collector;
	
	private int index = 0;
	
	private String[] sentences={
			"my dog has fleas",
			"i like cold beverages",
			"the dog ate my homework",
			"don't have a cow man",
			"i don't think i like fleas"
			};
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void close() {
		System.out.println("------------word spout stop------------");
	}

	@Override
	public void activate() {
		System.out.println("------------word spout activate------------");
	}

	@Override
	public void deactivate() {
		System.out.println("------------word spout deactivate------------");
	}

	@Override
	public void nextTuple() {
		this.collector.emit(new Values(sentences[index]));
		index ++;
		if(index >= sentences.length){
			index = 0;
		}
		Utils.waitForSeconds(1);
	}

	@Override
	public void ack(Object msgId) {
		System.out.println("------------word spout ack------------");
	}

	@Override
	public void fail(Object msgId) {
		System.out.println("------------word spout fail------------");
		System.out.println(msgId);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
