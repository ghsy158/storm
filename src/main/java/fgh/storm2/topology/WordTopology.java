package fgh.storm2.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import fgh.storm2.bolt.WordCountBolt;
import fgh.storm2.bolt.WordReportBolt;
import fgh.storm2.bolt.WordSplitBolt;
import fgh.storm2.spout.WordSpout;
import fgh.util.Utils;

/**
 * 
 * @author fgh
 * @since 2016年8月14日下午4:39:27
 */
public class WordTopology {

	public static final String WORD_SPOUT_ID = "word-spout";
	public static final String SPLIT_BOLT_ID = "word-split";
	public static final String COUNT_BOLT_ID = "word-count";
	public static final String REPORT_BOLT_ID = "word-report";

	public static void main(String[] args) {
		WordSpout spout = new WordSpout();
		WordSplitBolt splitBolt = new WordSplitBolt();
		WordCountBolt countBolt = new WordCountBolt();
		WordReportBolt reportBolt = new WordReportBolt();

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(WORD_SPOUT_ID, spout);
		builder.setBolt(SPLIT_BOLT_ID, splitBolt, 5).shuffleGrouping(WORD_SPOUT_ID);

		builder.setBolt(COUNT_BOLT_ID, countBolt, 5).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
		builder.setBolt(REPORT_BOLT_ID, reportBolt, 10).globalGrouping(COUNT_BOLT_ID);

		Config config = new Config();
		config.setNumWorkers(2);
		config.setDebug(true);

		// 本地模式
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("word-top", config, builder.createTopology());
		
		Utils.waitForSeconds(30);
		
		cluster.killTopology("word-top");
		cluster.shutdown();

	}
}
