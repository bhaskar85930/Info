package com.maprdemo.multiclusterdb;

import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class TableUtil {
    
	public static Get buildGet(String key, ArrayList<byte[]> parms) {
		
		Get g = new Get(Bytes.toBytes(key));
		for (byte[] p : parms)
			g.addFamily(p);
		
		return g;
	}
	
	public static Scan buildScan(String from, String to, ArrayList<byte[]> parms) {
		
		Scan s = new Scan(Bytes.toBytes(from), Bytes.toBytes(to));
		for (byte[] p : parms)		
			s.addFamily(p);
		
		return s;
	}
	
}
