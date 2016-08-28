package fgh.storm.redis;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * 
 * @author fgh
 * @since 2016年8月28日上午10:47:00
 */
public class SampleSpout extends BaseRichSpout {

	private static final long serialVersionUID = 5374635146774908078L;

	private SpoutOutputCollector collector;

	private static final Map<Integer, String> FIRST_NAME = new HashMap<Integer, String>();

	static {
		FIRST_NAME.put(0, "john");
		FIRST_NAME.put(1, "zhang");
		FIRST_NAME.put(2, "li");
		FIRST_NAME.put(3, "wang");
		FIRST_NAME.put(4, "liu");
	}

	private static final Map<Integer, String> LAST_NAME = new HashMap<Integer, String>();

	static {
		LAST_NAME.put(0, "san");
		LAST_NAME.put(1, "si");
		LAST_NAME.put(2, "bei");
		LAST_NAME.put(3, "david");
		LAST_NAME.put(4, "lilly");
	}
	private static final Map<Integer, String> COMPANY_NAME = new HashMap<Integer, String>();

	static {
		COMPANY_NAME.put(0, "abc");
		COMPANY_NAME.put(1, "ddsd");
		COMPANY_NAME.put(2, "gfds");
		COMPANY_NAME.put(3, "ggg");
		COMPANY_NAME.put(4, "nnn");
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		final Random random = new Random();
		int randomNumber = random.nextInt(5);
		collector.emit(
				new Values(FIRST_NAME.get(randomNumber), LAST_NAME.get(randomNumber), COMPANY_NAME.get(randomNumber)));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("firstName","lastName","companyName"));
	}

}
