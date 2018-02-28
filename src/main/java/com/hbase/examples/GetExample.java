package com.hbase.examples;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.utils.HBaseHelper;

public class GetExample {

	public static void main(String args[]) {
		Configuration conf = null;

		try {
			conf = HBaseConfiguration.create(); 
			HBaseHelper helper = HBaseHelper.getHelper(conf);
			String tableName = "testtable";
			helper.dropTable(tableName);
			helper.createTable(tableName, "colfam1","colfam2");


			HTable table;
			table = new HTable(conf, "testtable");
			Put put = new Put(Bytes.toBytes("row1"));
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
					Bytes.toBytes("val1"));
			table.put(put);

			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
					Bytes.toBytes("val2"));
			table.put(put);

			Get get = new Get(Bytes.toBytes("row1"));
			get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
			Result result = table.get(get);
			byte[] val = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
			
			System.out.println("Value: "+Bytes.toString(val));
			
			table.close();
			helper.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
