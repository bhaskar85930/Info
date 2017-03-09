package com.maprdemo.multiclusterdb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

public class TableService {
	
	private static final ExecutorService taskExec = Executors.newCachedThreadPool();
	
	private Map<String,Table> tables = new HashMap<String,Table>();
	private Map<String,Integer> tablesSLA = new HashMap<String,Integer>();
	
	
	public TableService(String table) {

		try {
			Configuration conf = HBaseConfiguration.create();
			conf.set("mapr.hbase.default.db", "maprdb");

			Connection conn = ConnectionFactory.createConnection(conf);
			
			loadClusterConfig(conn,table);

		} catch (IOException e) {
			// TODO Auto-generated catch block
		}
	}
	
	//TODO: Capture processing time by cluster and table then send to process for calculation percentile
	//      to get processing timeout.
	public Result getFromTable(Get get) {
		long start_time = System.nanoTime();
		
		Result result = null;
		
		for (String cluster : tables.keySet()) {
			//System.out.println("Working on cluster: " + cluster);
			GetFromTable gft = new GetFromTable(get,tables.get(cluster));
		
	        result = getFromTable(gft, tablesSLA.get(cluster).longValue());
	        if (result != null) {
	        	System.out.println("Processing Time: " + (System.nanoTime() - start_time)/1e6);
	        	return result; 
	        }
		}
 
		return null;
	}

	public ResultScanner scanTable(Scan s) {
		long start_time = System.nanoTime();
		
		ResultScanner result = null;
		
		for (String cluster : tables.keySet()) {
			//System.out.println("Working on cluster: " + cluster);
			ScanTable st = new ScanTable(s,tables.get(cluster));
			
	        result = scanTable(st, tablesSLA.get(cluster).longValue());
	        if (result != null) {
	        	System.out.println("Processing Time: " + (System.nanoTime() - start_time)/1e6);
	        	return result; 
	        }
		}
 
		return null;
	}
	
	private Result getFromTable(GetFromTable gft, long timeout) {
    		
        Future<Result> task = taskExec.submit(gft);
        
        try {
        	Result result = (Result) task.get(timeout, MILLISECONDS);
            return result;
        } catch (TimeoutException e) {
            // task will be cancelled below
        	System.out.println("Time out on Table: " + gft.getTable());
        	e.printStackTrace();
        } catch (ExecutionException e) {
            // exception thrown in task; 
            e.printStackTrace();
        } catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
            // Harmless if task already completed
            task.cancel(true); // interrupt if running
        }

        return null;
	}
	
	private ResultScanner scanTable(ScanTable st, long timeout) {
		
        Future<ResultScanner> task = taskExec.submit(st);
        
        try {
        	ResultScanner result = (ResultScanner) task.get(timeout, MILLISECONDS);
            return result;
        } catch (TimeoutException e) {
            // task will be cancelled below
        	System.out.println("Time out on Table: " + st.getTable());
        	e.printStackTrace();
        } catch (ExecutionException e) {
            // exception thrown in task; 
            e.printStackTrace();
        } catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
            // Harmless if task already completed
            task.cancel(true); // interrupt if running
        }

        return null;
	}
	
	private void loadClusterConfig(Connection conn,String table) {
		
		try {
	 		// Open the input file and read it line by line
			BufferedReader br = new BufferedReader(new FileReader("./clusters.conf"));
			String currentLine;
			String[] tokens; // an array to hold values from one line of the file
	
			while ((currentLine = br.readLine()) != null) {
				System.out.println(currentLine);
				tokens = currentLine.split("\\s*:\\s*"); // Split on : boundaries, stripping white space.
				if ( (tokens != null) && (tokens.length == 3) )  {
					tables.put(tokens[0], conn.getTable(TableName.valueOf(tokens[1] + table)));
					tablesSLA.put(tokens[0], new Integer(tokens[2]));
				} else {
					System.out.println("Ignoring malformed line: " + currentLine);
				}
			}
			
			// Close the file & exit.
			if (br != null) br.close();
			
		} catch (IOException e) {
			System.out.println("No cluster configuration file found.");
		} 

	}
}
