package fgh.storm1.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import fgh.storm1.bolt.PrintBolt;
import fgh.storm1.bolt.WriteBolt;
import fgh.storm1.spout.PWSpout;


/**
 * 分组模式
 * @author fgh
 * @since 2016年7月24日下午4:40:16
 */
public class PWTopology3 {

	public static void main(String[] args)
			throws InterruptedException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {

		Config config = new Config();
		config.setNumWorkers(2);
		config.setDebug(true);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new PWSpout(),4);// setNumTasks(2)
		//设置blob的并行度和任务数 2个执行器和4个任务
		builder.setBolt("print-bolt", new PrintBolt(),4).shuffleGrouping("spout");
		//设置随机分组
//		builder.setBolt("write-bolt", new WriteBolt(),8).shuffleGrouping("print-bolt");
		//设置字段分组
//		builder.setBolt("write-bolt", new WriteBolt(),8).fieldsGrouping("print-bolt", new Fields("write"));
		//设置广播分组
//		builder.setBolt("write-bolt", new WriteBolt(),3).allGrouping("print-bolt");
		//设置全局分组 只会发给其中一个taskid最低的bolt
		builder.setBolt("write-bolt", new WriteBolt(),4).globalGrouping("print-bolt");
		
		// 本地模式
		 LocalCluster cluster = new LocalCluster();
		 cluster.submitTopology("top3", config, builder.createTopology());

		// 集群模式
//		StormSubmitter.submitTopology("top2", config, builder.createTopology());

	}
}
