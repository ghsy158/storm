package fgh.storm1.spout;

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
 * Spout
 * 
 * @author fgh
 * @since 2016年7月24日下午5:12:10
 */
public class PWSpout extends BaseRichSpout {

	private static final long serialVersionUID = 8398255343844926800L;

	private SpoutOutputCollector collector;

	private static Map<Integer, String> map = new HashMap<Integer, String>();

	static {
		map.put(0, "java");
		map.put(1, "php");
		map.put(2, "python");
		map.put(3, "C++");
		map.put(4, "ruby");
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// 对spout初始化
		this.collector = collector;
		System.out.println(this.collector);
	}

	/**
	 * 
	 * <b>方法描述：</b>轮询tuple
	 * 
	 * @param
	 */
	@Override
	public void nextTuple() {
		// 随机发送一个单词
		final Random r = new Random();
		int num = r.nextInt(5);
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		this.collector.emit(new Values(map.get(num)));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//发送单词的name叫print，下一个通过print这个key获取nextTuple发送的值
		declarer.declare(new Fields("print"));
	}

}
