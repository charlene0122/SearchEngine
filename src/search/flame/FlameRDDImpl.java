package search.flame;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import search.flame.FlamePairRDD.TwoStringsToString;
import search.kvs.Row;
import search.tools.Hasher;
import search.tools.Serializer;

public class FlameRDDImpl implements FlameRDD {
	String tableName;
	FlameContextImpl context;
	boolean isDestroyed;

	public FlameRDDImpl(FlameContextImpl context, String tableName) {
		this.tableName = tableName;
		this.context = context;
		this.isDestroyed = false;
	}

	public String getTable() {
		return tableName;
	}

	@Override
	public List<String> collect() throws Exception {
		checkIfDestroyed();
		List<String> results = new LinkedList<>();
		Iterator<Row> iter = context.getKVS().scan(tableName);
		while (iter.hasNext()) {
			Row row = iter.next();
			results.add(row.get("value"));
		}

		return results;
	}

	@Override
	public FlameRDD flatMap(StringToIterable lambda, boolean persistent) throws Exception {
		checkIfDestroyed();
		byte[] lambdaAsBytes = Serializer.objectToByteArray(lambda);
		return (FlameRDD) context.invokeOperation(tableName, lambdaAsBytes, "flatMap", null, "rdd", persistent);
	}

	@Override
	public FlamePairRDD mapToPair(StringToPair lambda, boolean persistent) throws Exception {
		checkIfDestroyed();
		byte[] lambdaAsBytes = Serializer.objectToByteArray(lambda);
		return (FlamePairRDD) context.invokeOperation(tableName, lambdaAsBytes, "mapToPair", null, "rdd", persistent);
	}

	@Override
	public FlameRDD intersection(FlameRDD r, boolean persistent) throws Exception {
		checkIfDestroyed();
		return (FlameRDD) context.invokeOperation(tableName, null, "intersection", ((FlameRDDImpl) r).getTable(), "rdd",
				persistent);
	}

	@Override
	public FlameRDD sample(double f, boolean persistent) throws Exception {
		checkIfDestroyed();
		return (FlameRDD) context.invokeOperation(tableName, null, "sample", String.valueOf(f), "rdd", persistent);
	}

	@Override
	public int count() throws Exception {
		checkIfDestroyed();
		return context.getKVS().count(tableName);
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		checkIfDestroyed();
		context.getKVS().rename(tableName, tableNameArg);
		this.tableName = tableNameArg;
	}

	@Override
	public FlameRDD distinct() throws Exception {
		checkIfDestroyed();
		Iterator<Row> iter = context.getKVS().scan(tableName);
		String newTableName = "distinct_" + tableName;
		Set<String> uniqueValues = new HashSet<>();
		int index = 1;
		while (iter.hasNext()) {
			Row row = iter.next();
			String value = row.get("value");
			if (uniqueValues.add(value)) {
				String rowKey = Hasher.hash(newTableName + String.valueOf(index++));
				context.getKVS().put(newTableName, rowKey, "value", value);
			}
		}
		return new FlameRDDImpl(context, newTableName);
	}

	@Override
	public void destroy() throws Exception {
		checkIfDestroyed();
		context.getKVS().delete(tableName);
		isDestroyed = true;
	}

	@Override
	public Vector<String> take(int num) throws Exception {
		checkIfDestroyed();
		Vector<String> elements = new Vector<>();
		Iterator<Row> iter = context.getKVS().scan(tableName);
		int i = 0;
		while (iter.hasNext() && i < num) {
			Row row = iter.next();
			for (String column : row.columns()) {
				elements.add(row.get(column));
			}
			i++;
		}
		return elements;
	}

	@Override
	public String fold(String zeroElement, TwoStringsToString lambda, boolean persistent) throws Exception {
		checkIfDestroyed();
		byte[] lambdaAsBytes = Serializer.objectToByteArray(lambda);
		String result = (String) context.invokeOperation(tableName, lambdaAsBytes, "fold", zeroElement, "rdd",
				persistent);
		return result;
	}

	@Override
	public FlamePairRDD flatMapToPair(StringToPairIterable lambda, boolean persistent) throws Exception {
		checkIfDestroyed();
		byte[] lambdaAsBytes = Serializer.objectToByteArray(lambda);
		return (FlamePairRDD) context.invokeOperation(tableName, lambdaAsBytes, "flatMapToPair", null, "rdd",
				persistent);
	}

	private void checkIfDestroyed() throws Exception {
		if (isDestroyed) {
			throw new Exception("Operation not allowed: The RDD has been destroyed.");
		}
	}

	@Override
	public FlameRDD filter(StringToBoolean lambda) throws Exception {
		return null;
	}

	@Override
	public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
		return null;
	}

	@Override
	public FlamePairRDD groupBy(StringToString lambda) throws Exception {
		checkIfDestroyed();
		return null;
	}

}
