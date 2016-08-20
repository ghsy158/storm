package fgh.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Trident Filter 
 * @author fgh
 * @since 2016年8月20日下午4:42:15
 */
public class TridentFilter {

	/**
	 * 过滤器 只有前两个的和是2的倍数的时候才会输出
	 * @author fgh
	 * @since 2016年8月20日下午4:43:51
	 */
	public static class CheckEvenSumFilter extends BaseFilter {
		private static final long serialVersionUID = 7L;

		public boolean isKeep(TridentTuple tuple) {
			System.out.println("CheckEvenSumFilter,接收到的内容:" + tuple);
			int number1 = tuple.getInteger(0);
			int number2 = tuple.getInteger(1);
			int sum = number1 + number2;
			if (sum % 2 == 0) {
				return true;
			}
			return false;
		}
	}
	
	public static class Result extends BaseFunction {

		private static final long serialVersionUID = -2115314893607611092L;

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			System.out.println("Result,接收到的内容:" + tuple);
			int a = tuple.getIntegerByField("a");
			int b = tuple.getIntegerByField("b");
			int c = tuple.getIntegerByField("c");
			int d = tuple.getIntegerByField("d");
			System.out.println("a=" + a + ",b=" + b + ",c=" + c + ",d=" + d);
		}

	}

	@SuppressWarnings("unchecked")
	public static  StormTopology buildTopology(){
		TridentTopology topology = new TridentTopology();
		//设定数据源
		FixedBatchSpout spout = new FixedBatchSpout(
				new Fields("a","b","c","d"),//声明输入的域字段
				4,//设置批处理大小
				//设置数据源内容
				new Values(1,4,7,10),
				new Values(1,1,3,11),
				new Values(2,2,7,1),
				new Values(2,5,7,2));
		
//		指定是否循环
		spout.setCycle(false);
		//指定输入源spout
		Stream inputStream = topology.newStream("spout", spout);
		
		/**
		 * 要实现spout-bolt模式	 在trident中是用each来做的
		 * each 方法参数:
		 * 1、输入数据源参数名称
		 * 2 需要流转执行的function对象,
		 * 3 指定function对象里的输出参数名称sum
		 */
		inputStream.each(new Fields("a","b","c","d"), new CheckEvenSumFilter())
		
		/**
		 * 继续使用each调用下一个function(bolt)
		 * 第一个参数:"a","b","c","d","sum"
		 * 第二个参数:new Result() 也就是执行函数
		 * 第三个参数为没有输出
		 */
		.each(new Fields("a","b","c","d"), new Result(),new Fields());
		
		return topology.build();
	}
	
	
	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		Config conf = new Config();
		conf.setNumWorkers(2);
		conf.setMaxSpoutPending(20);
		if(args.length==0){
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("trident-function", conf, buildTopology());
			Thread.sleep(100000);
			cluster.shutdown();
		}else{
			StormSubmitter.submitTopology(args[0], conf, buildTopology());
		}
	}
}
