package search.Spark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import search.kvs.KVSClient;
import search.kvs.Row;
import search.tools.Serializer;

public class SparkPairRDDImpl implements SparkPairRDD {

	String tableName;
	KVSClient kvs;
	SparkContextImpl context;
	boolean isDestroyed;

	public SparkPairRDDImpl(SparkContextImpl context, String table) {
		this.tableName = table;
		this.context = context;
		this.kvs = Coordinator.kvs;
		this.isDestroyed = false;
	}

	@Override
	public List<SparkPair> collect() throws Exception {
		checkIfDestroyed();
		List<SparkPair> results = new ArrayList<>();
		Iterator<Row> rows = kvs.scan(tableName);
		while (rows.hasNext()) {
			Row row = rows.next();
			for (String column : row.columns()) {
				SparkPair pair = new SparkPair(row.key(), row.get(column));
				results.add(pair);
			}
		}
		return results;
	}

	@Override
	public SparkPairRDD foldByKey(String zeroElement, TwoStringsToString lambda, boolean persistent) throws Exception {
		checkIfDestroyed();
		byte[] lambdaAsBytes = Serializer.objectToByteArray(lambda);
		return (SparkPairRDD) context.invokeOperation(tableName, lambdaAsBytes, "foldByKey", zeroElement, "rdd",
				persistent);
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		checkIfDestroyed();
		kvs.rename(tableName, tableNameArg);
		this.tableName = tableNameArg;
	}

	@Override
	public SparkRDD flatMap(PairToStringIterable lambda, boolean persistent) throws Exception {
		checkIfDestroyed();
		byte[] lambdaAsBytes = Serializer.objectToByteArray(lambda);
		System.out.println("before flatmap invoke operation");
		return (SparkRDD) context.invokeOperation(tableName, lambdaAsBytes, "flatMapFromPair", null, "rdd", persistent);
	}

	@Override
	public void destroy() throws Exception {
		checkIfDestroyed();
		kvs.delete(tableName);
		isDestroyed = true;
	}

	@Override
	public SparkPairRDD flatMapToPair(PairToPairIterable lambda, boolean persistent) throws Exception {
		checkIfDestroyed();
		byte[] lambdaAsBytes = Serializer.objectToByteArray(lambda);
		return (SparkPairRDD) context.invokeOperation(tableName, lambdaAsBytes, "flatMapToPairFromPair", null, "rdd",
				persistent);
	}

	@Override
	public SparkPairRDD join(SparkPairRDD other, boolean persistent) throws Exception {
		checkIfDestroyed();
		SparkPairRDDImpl otherImpl = (SparkPairRDDImpl) other;
		String otherTable = otherImpl.tableName;
		byte[] otherTableAsBytes = Serializer.objectToByteArray(otherTable);
		return (SparkPairRDD) context.invokeOperation(this.tableName, otherTableAsBytes, "join", otherTable, "rdd",
				persistent);
	}

	private void checkIfDestroyed() throws Exception {
		if (isDestroyed) {
			throw new Exception("Operation not allowed: The RDD has been destroyed.");
		}
	}

	@Override
	public SparkPairRDD cogroup(SparkPairRDD other) throws Exception {
		return null;
	}

}
