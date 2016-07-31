package fgh.storm1.bolt;

import java.io.File;
import java.io.FileWriter;

import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * WriteBolt
 * 
 * @author fgh
 * @since 2016年7月24日下午4:46:35
 */
public class WriteBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 2391960065875597170L;

	private static final Logger log = Logger.getLogger(WriteBolt.class);

	private FileWriter writer;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// 获取上一个组件声明的field
		String text = input.getStringByField("write");
		try {
			if (writer == null) {
				String osName = System.getProperty("os.name");
				if (osName.equals("Windows 10")) {
					writer = new FileWriter(new File("d:/storm_test/" + this));
				} else if (osName.equals("Windows 8.1")) {
					writer = new FileWriter(new File("d:/storm_test/" + this));
				} else if (osName.equals("Windows 7")) {
					writer = new FileWriter(new File("d:/storm_test/" + this));
				} else if (osName.equals("Linux")) {
					writer = new FileWriter(new File("/usr/local/temp/storm_test/" + this));
				}
			}
			log.info("【write】:写入文件");
			log.info("【write】:接收的内容" + text);
			writer.write(text);
			writer.write("\r\n");
			writer.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
