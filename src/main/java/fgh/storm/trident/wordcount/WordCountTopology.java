package fgh.storm.trident.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * 
 * @author fgh
 * @since 2016年8月21日下午12:42:06
 */
public class WordCountTopology {

	
	
	@SuppressWarnings("unchecked")
	public static StormTopology buildTopology(){
		TridentTopology topology  = new TridentTopology();
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("subjects"), 4,
				
				new Values("java java php ruby c++"),
				new Values("java python ruby php c++"),
				new Values("java java php hadoop html5"),
				new Values("java java php ruby c++"));
		spout.setCycle(false);
		Stream stream = topology.newStream("spout", spout);
		
		//设置随机分组
		stream.shuffle()
			.each(new Fields("subjects"), new SplitFunction(),new Fields("sub"))
			//进行分组操作 参数为分组字段sub,类似于之前的FieldsGroup
			.groupBy(new Fields("sub"))
			//对分组之后的结果进行聚合操作, 参数1位聚合方法count函数,参数2为输出字段名
			.aggregate(new Count(), new Fields("count"))
			//继续使用each调用下一个function（bolt） 输入参数为sub 和count,第二个参数为要执行的function，第三个参数说明不输出
			.each(new Fields("sub","count"), new ResultFunction(),new Fields())
			.parallelismHint(1);//并行度为1 
		
		return topology.build();
		
	}
	
	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		Config conf = new Config();
		conf.setNumWorkers(2);
		conf.setMaxSpoutPending(10);
		if(args.length==0){
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("trident-wordcount", conf, buildTopology());
			Thread.sleep(1000000);
			cluster.shutdown();
		}else{
			StormSubmitter.submitTopology(args[0], conf, buildTopology());
		}
	}
}
