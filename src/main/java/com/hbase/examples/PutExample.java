package com.hbase.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.utils.HBaseHelper;

public class PutExample {
	public static void main(String args[]) {

		try { 
			Configuration conf = HBaseConfiguration.create(); 
			HBaseHelper helper = HBaseHelper.getHelper(conf);
			helper.dropTable("testtable");
		    helper.createTable("testtable", "colfam1");
		    Connection connection = ConnectionFactory.createConnection(conf);
		    Table table = connection.getTable(TableName.valueOf("testtable"));

		    Put put = new Put(Bytes.toBytes("row1"));

		    put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
		      Bytes.toBytes("val1"));
		    put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
		      Bytes.toBytes("val2"));

		    table.put(put); 
		    table.close();
		    connection.close();

		    helper.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
