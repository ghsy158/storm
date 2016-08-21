package fgh.storm.trident.wordcount;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * word count Result Function
 * @author fgh
 * @since 2016年8月21日下午12:50:58
 */
public class ResultFunction extends BaseFunction {

	private static final long serialVersionUID = 6683849638044683377L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String sub = tuple.getStringByField("sub");
		long count = tuple.getLongByField("count");
		System.out.println(sub + ":" + count);
	}

}
