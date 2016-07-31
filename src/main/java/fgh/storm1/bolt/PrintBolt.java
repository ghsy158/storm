package fgh.storm1.bolt;

import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * PrintBolt
 * @author fgh
 * @since 2016年7月24日下午4:45:50
 */
public class PrintBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -4149201897782907793L;

	private static final Logger log = Logger.getLogger(PrintBolt.class);

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// 获取上一个组件声明的field
		String print = input.getStringByField("print");
		log.info("【print】:" + print);
		// 进行传递给下一个bolt
		collector.emit(new Values(print));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("write"));
	}

}
