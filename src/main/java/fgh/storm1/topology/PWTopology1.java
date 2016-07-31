package fgh.storm1.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import fgh.storm1.bolt.PrintBolt;
import fgh.storm1.bolt.WriteBolt;
import fgh.storm1.spout.PWSpout;


/**
 * 
 * @author fgh
 * @since 2016年7月24日下午4:40:16
 */
public class PWTopology1 {

	public static void main(String[] args)
			throws InterruptedException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {

		Config config = new Config();
		config.setNumWorkers(2);
		config.setDebug(false);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new PWSpout());
		builder.setBolt("print-bolt", new PrintBolt()).shuffleGrouping("spout");
		builder.setBolt("write-bolt", new WriteBolt()).shuffleGrouping("print-bolt");

		// if (args != null && args.length > 0) {
		// config.setNumWorkers(1);
		// StormSubmitter.submitTopology(args[0], config,
		// builder.createTopology());
		// } else {
		// // 这里是本地模式下运行的启动代码。
		// config.setMaxTaskParallelism(1);
		// LocalCluster cluster = new LocalCluster();
		// cluster.submitTopology("top1", config, builder.createTopology());
		//// cluster.killTopology("top1");
		//// cluster.shutdown();
		// }

		// 本地模式
//		 LocalCluster cluster = new LocalCluster();
//		 cluster.submitTopology("top1", config, builder.createTopology());
//		 Thread.sleep(10000);

		// 集群模式
		StormSubmitter.submitTopology("top1_cluster", config, builder.createTopology());

	}
}
