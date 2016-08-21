package fgh.storm.trident.wordcount;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * 
 * @author fgh
 * @since 2016年8月21日下午12:47:33
 */
public class SplitFunction extends BaseFunction{

	private static final long serialVersionUID = 1L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String subjects = tuple.getStringByField("subjects");
		for(String sub:subjects.split(" ")){
			//逻辑处理  然后 发射到下一个组件。不用管输出字段的设置
			collector.emit(new Values(sub));
		}
		
	}

}
