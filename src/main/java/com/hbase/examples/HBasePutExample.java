package com.hbase.examples;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author Shobha
 * 
 */

public class HBasePutExample {
	public static void main(String args[]) {
		try {
			HBaseHelper.createHadoopConfiguration();
			Configuration conf = HBaseHelper.conf;
			HTable table;
			table = new HTable(conf, "testtable");
			Put put = new Put(Bytes.toBytes("row1"));
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
			Bytes.toBytes("val1"));
			table.put(put);

			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
			Bytes.toBytes("val2"));
			table.put(put);
			
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
