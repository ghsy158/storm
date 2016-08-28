package fgh.storm.redis;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * 
 * @author fgh
 * @since 2016年8月28日上午10:46:40
 */
public class Topology {

	public static void main(String[] args) throws InterruptedException {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new SampleSpout(), 2);
		builder.setBolt("bolt", new StormRedisBolt("192.168.0.201", 6379), 2).shuffleGrouping("spout");

		Config conf = new Config();
		conf.setDebug(true);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("stormRedisTop", conf, builder.createTopology());

//		Thread.sleep(500000);
//
//		cluster.killTopology("stormRedisTop");
//
//		cluster.shutdown();
	}
}
