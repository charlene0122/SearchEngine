package cis5550.flame;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.*;
import static cis5550.flame.Coordinator.getServer;
import static cis5550.generic.Coordinator.*;

public class FlameContextImpl implements FlameContext, Serializable {


	private String jarName;
	private StringBuilder output;
	private int sequenceNumber;
	private int concurrencyLevel;


	public FlameContextImpl(String jarName) {
	    this.jarName = jarName;
	    this.output = new StringBuilder();
	    this.sequenceNumber = 0;
	    this.concurrencyLevel = 1;
	}
	
	@Override
	public KVSClient getKVS() {
        return Coordinator.kvs;
	}
	
	@Override
	public void output(String s) {
	    output.append(s);
	}
	
	public String getOutput() {
		if (output.length() == 0) {
            return "";
        } else {
            return output.toString();
        }
    }

	@Override
	public FlameRDD parallelize(List<String> list) throws Exception {
        String tableName = "job_" + System.currentTimeMillis() + "_" + (sequenceNumber++);

        for (int i = 0; i < list.size(); i++) {
            String rowKey = Hasher.hash(tableName + String.valueOf(i+1));
            System.out.println("RDD parallelize, table name: " + tableName + " row key: " + rowKey);
            Coordinator.kvs.put(tableName, rowKey, "value", list.get(i));
        }
        return new FlameRDDImpl(this, tableName);
	}
	
	public Object invokeOperation(String inputTable, byte[] lambda, String operation, String argument, String route, boolean persistent) throws Exception {
		String outputTable = "output_" + System.currentTimeMillis() + "_" + (sequenceNumber++);
        if (persistent) {
            outputTable = "pt-" + outputTable;
        }
        Partitioner partitionerHandler = new Partitioner();
        partitionerHandler.setKeyRangesPerWorker(concurrencyLevel);
       
        int totalWorkers = Coordinator.kvs.numWorkers();

    	for (int i = 0; i < totalWorkers - 1; i++) {
            partitionerHandler.addKVSWorker(Coordinator.kvs.getWorkerAddress(i), Coordinator.kvs.getWorkerID(i), Coordinator.kvs.getWorkerID(i + 1));
        }
        int lastWorker = totalWorkers- 1;
        partitionerHandler.addKVSWorker(Coordinator.kvs.getWorkerAddress(lastWorker), Coordinator.kvs.getWorkerID(lastWorker), null);
        partitionerHandler.addKVSWorker(Coordinator.kvs.getWorkerAddress(lastWorker), null, Coordinator.kvs.getWorkerID(0));

        System.out.println("flame context flameworkers count: " + Coordinator.getWorkers());
        for (String worker : Coordinator.getWorkers()) {
            partitionerHandler.addFlameWorker(worker);
        }

        Vector<Partitioner.Partition> partitionSets = partitionerHandler.assignPartitions();
        System.out.println("invoke operation:  operation: " + operation + ", partitions: " + partitionSets);



        AtomicBoolean errorFlag = new AtomicBoolean(false);
        int workerCount = partitionSets.size();
        String results[] = new String[workerCount];
        Thread[] workerThreads = new Thread[workerCount];

        for (int i = 0; i < workerCount; i++) {
            Partitioner.Partition individualPartition = partitionSets.get(i);
            String startKey = individualPartition.fromKey == null ? "!!" : individualPartition.fromKey;
            String endKeyExclusive = individualPartition.toKeyExclusive == null ? "!!" : individualPartition.toKeyExclusive;
            String flameWorkerID = individualPartition.assignedFlameWorker;
            
            StringBuilder url = new StringBuilder();
            
            url.append("http://").append(getServer()).append("/"+ route +"?worker=").append(flameWorkerID)
            .append("&oper=").append(operation).append("&input=").append(inputTable).append("&output=")
            .append(outputTable).append("&from=").append(startKey).append("&to=").append(endKeyExclusive)
            .append("&jar=").append(jarName);
            
           
            if (argument != null) {
                switch (operation) {
                    case "foldByKey":                    	
                    	url.append("&zero=" + argument);
                    	break;
                    case "fold":                    	
                    	url.append("&zero2=" + argument);
                    	break;
                    case "intersection":
                    	url.append("&that=" + argument);
                    	break;
                    case "sample":
                    	url.append("&prob=" + argument);
                    	break;
                    case "join":
                        url.append("&otherTable=" + argument);
                        break;
                    }
                }
            
            final int currentIndex = i;
            final String finalURL = url.toString();
            workerThreads[i] = new Thread(() -> {
                try {
                    results[currentIndex] = String.valueOf(HTTP.doRequest("POST", finalURL, lambda).statusCode());
                    if (!"OK".equals(results[currentIndex])) {
                        errorFlag.set(true);
                    }
                } catch (IOException ex) {
                    errorFlag.set(true);
                }
                
            });

            workerThreads[i].start();
        }
 
        for (Thread thread : workerThreads) {
            thread.join();
        }

        if (Arrays.asList("mapToPair", "groupBy", "foldByKey", "flatMapToPair", "flatMapToPairFromPair", "join").contains(operation)) {
        	return new FlamePairRDDImpl(this, outputTable);
        } else if (operation != null && operation.equals("fold")) {
        	Iterator<Row> rows = Coordinator.kvs.scan(outputTable);
        	if (rows.hasNext()) {
        		Row row = rows.next();
        		return row.get("value");
        	}
        }
            
        return new FlameRDDImpl(this, outputTable);
	}

	@Override
	public FlameRDD fromTable(String tableName, RowToString lambda, boolean persistent) throws Exception {
		byte[] lambdaAsBytes = Serializer.objectToByteArray(lambda);
	    return (FlameRDD) invokeOperation(tableName, lambdaAsBytes, null, null, "fromTable", persistent);
	}

	@Override
	public void setConcurrencyLevel(int keyRangesPerWorker) {
		if (keyRangesPerWorker <= 0) {
            throw new IllegalArgumentException("Concurrency level must be a positive number.");
        }
        this.concurrencyLevel = keyRangesPerWorker;
	}

}
