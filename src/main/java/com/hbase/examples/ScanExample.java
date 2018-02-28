package com.hbase.examples;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Shobha
 * 
 */

public class ScanExample {
	
	public static void main(String[] args) throws IOException {

		Configuration conf = HBaseConfiguration.create(); 
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");
		helper.createTable("testtable", "colfam1", "colfam2");
		System.out.println("Adding rows to table...");
		HTable table = new HTable( conf, "testtable" );
		int n = 20;
		
		String[] keys = new String[n];
		String[] qualifier = new String[n];
		long[] ts = new long[n];
		String[] vals = new String[n];

		for(int i=0;i<n;i++){
			keys[i]="row-"+i;
			qualifier[i]="col-"+i;
			vals[i]=(i+100)+"";
		}
		
		String[] colFamily = new String[2];
		colFamily[0]="colfam"+1;
		colFamily[1]="colfam"+2;
		
		//helper.put("testtable", keys, colFamily, qualifier, ts,vals);


		System.out.println("Scanning table #1...");
		Scan scan1 = new Scan();
		ResultScanner scanner1 = table.getScanner(scan1);
		for (Result res : scanner1) {
			System.out.println(res+"");
		}
		scanner1.close();

		System.out.println("Scanning table #2...");
		Scan scan2 = new Scan();
		scan2.addFamily(Bytes.toBytes("colfam1")); 
		ResultScanner scanner2 = table.getScanner(scan2);
		for (Result res : scanner2) {
			System.out.println(res+"");
		}
		scanner2.close();

		System.out.println("Scanning table #3...");
		Scan scan3 = new Scan();
		scan3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-1")).
		addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("col-12")).
		setStartRow(Bytes.toBytes("row-5")).
		setStopRow(Bytes.toBytes("row-10"));
		ResultScanner scanner3 = table.getScanner(scan3);
		for (Result res : scanner3) {
			System.out.println(res+"");
		}
		scanner3.close();

		System.out.println("Scanning table #4...");
		Scan scan4 = new Scan();
		scan4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5")). 
		setStartRow(Bytes.toBytes("row-10")).
		setStopRow(Bytes.toBytes("row-20"));
		ResultScanner scanner4 = table.getScanner(scan4);
		for (Result res : scanner4) {
			System.out.println(res+"");
		}
		scanner4.close();

		System.out.println("Scanning table #5...");
		Scan scan5 = new Scan();
		scan5.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5")).
		setStartRow(Bytes.toBytes("row-20")).
		setStopRow(Bytes.toBytes("row-10")).
		setReversed(true);
		ResultScanner scanner5 = table.getScanner(scan5);
		for (Result res : scanner5) {
			System.out.println(res+"");
		}
		scanner5.close();

		table.close();
		helper.close();
	}
	
}
