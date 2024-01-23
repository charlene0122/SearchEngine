package search.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import search.kvs.KVSClient;
import search.kvs.Row;
import search.tools.Serializer;

public class FlamePairRDDImpl implements FlamePairRDD {

	String tableName;
	KVSClient kvs;
	FlameContextImpl context;
	boolean isDestroyed;

	public FlamePairRDDImpl(FlameContextImpl context, String table) {
		this.tableName = table;
		this.context = context;
		this.kvs = Coordinator.kvs;
		this.isDestroyed = false;
	}

	@Override
	public List<FlamePair> collect() throws Exception {
		checkIfDestroyed();
		List<FlamePair> results = new ArrayList<>();
		Iterator<Row> rows = kvs.scan(tableName);
		while (rows.hasNext()) {
			Row row = rows.next();
			for (String column : row.columns()) {
				FlamePair pair = new FlamePair(row.key(), row.get(column));
				results.add(pair);
			}
		}
		return results;
	}

	@Override
	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda, boolean persistent) throws Exception {
		checkIfDestroyed();
		byte[] lambdaAsBytes = Serializer.objectToByteArray(lambda);
		return (FlamePairRDD) context.invokeOperation(tableName, lambdaAsBytes, "foldByKey", zeroElement, "rdd",
				persistent);
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		checkIfDestroyed();
		kvs.rename(tableName, tableNameArg);
		this.tableName = tableNameArg;
	}

	@Override
	public FlameRDD flatMap(PairToStringIterable lambda, boolean persistent) throws Exception {
		checkIfDestroyed();
		byte[] lambdaAsBytes = Serializer.objectToByteArray(lambda);
		System.out.println("before flatmap invoke operation");
		return (FlameRDD) context.invokeOperation(tableName, lambdaAsBytes, "flatMapFromPair", null, "rdd", persistent);
	}

	@Override
	public void destroy() throws Exception {
		checkIfDestroyed();
		kvs.delete(tableName);
		isDestroyed = true;
	}

	@Override
	public FlamePairRDD flatMapToPair(PairToPairIterable lambda, boolean persistent) throws Exception {
		checkIfDestroyed();
		byte[] lambdaAsBytes = Serializer.objectToByteArray(lambda);
		return (FlamePairRDD) context.invokeOperation(tableName, lambdaAsBytes, "flatMapToPairFromPair", null, "rdd",
				persistent);
	}

	@Override
	public FlamePairRDD join(FlamePairRDD other, boolean persistent) throws Exception {
		checkIfDestroyed();
		FlamePairRDDImpl otherImpl = (FlamePairRDDImpl) other;
		String otherTable = otherImpl.tableName;
		byte[] otherTableAsBytes = Serializer.objectToByteArray(otherTable);
		return (FlamePairRDD) context.invokeOperation(this.tableName, otherTableAsBytes, "join", otherTable, "rdd",
				persistent);
	}

	private void checkIfDestroyed() throws Exception {
		if (isDestroyed) {
			throw new Exception("Operation not allowed: The RDD has been destroyed.");
		}
	}

	@Override
	public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
		return null;
	}

}
