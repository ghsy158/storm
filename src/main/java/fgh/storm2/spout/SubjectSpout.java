package fgh.storm2.spout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * 
 * @author fgh
 * @since 2016年8月21日下午3:18:23
 */
public class SubjectSpout implements IBatchSpout {

	private static final long serialVersionUID = 3010670260293255783L;

	private int batchSize;

	// 容器
	private HashMap<Long, List<List<Object>>> batchesMap = new HashMap<Long, List<List<Object>>>();

	private static final Map<Integer, String> DATA_MAP = new HashMap<Integer, String>();

	static {
		DATA_MAP.put(0, "java ruby java php");
		DATA_MAP.put(1, "java python C++ php");
		DATA_MAP.put(2, "java C++ java python");
		DATA_MAP.put(3, "java ruby java java");
	}

	public SubjectSpout(int batchSize) {
		this.batchSize = batchSize;
	}

	@Override
	public void open(Map conf, TopologyContext context) {
	}

	/**
	 * <b>方法描述：</b>失败自动重发
	 * 
	 * @param @param
	 *            batchId
	 * @param @param
	 *            collector
	 */
	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		List<List<Object>> batches = new ArrayList<List<Object>>();
		for (int i = 0; i < this.batchSize; i++) {
			batches.add(new Values(DATA_MAP.get(i)));
		}
		System.out.println("batchId:" + batchId);
		this.batchesMap.put(batchId, batches);
		for (List<Object> list : batches) {
			collector.emit(list);
		}
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void ack(long batchId) {
		System.out.println("ack,remove batchId:" + batchId);
		this.batchesMap.remove(batchId);
	}

	@Override
	public void close() {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("subjects");
	}

}
