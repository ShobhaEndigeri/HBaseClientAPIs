package com.hbase.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * 
 * @author Shobha
 * 
 */

public class HBaseCreateTable {
	
	public static void main(String args[]) {
		HBaseAdmin admin = null;
		Configuration conf = null;
		
		try {

			HBaseHelper.createHadoopConfiguration();
			conf = HBaseHelper.conf;
			admin = new HBaseAdmin( conf );
			String table ="testtable";
			String[]  colfams = {"colFam1"};

			TableName tableName = TableName.valueOf(table); 
			if(admin.tableExists( tableName )) {
				admin.disableTable( tableName );
				admin.deleteTable(tableName);
			}

			HTableDescriptor desc = new HTableDescriptor( table );
			for( String cf : colfams )
			{
				HColumnDescriptor coldef = new HColumnDescriptor( cf );
				desc.addFamily( coldef );
			}

			admin.createTable( desc );

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(admin != null) {
				try {
					admin.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}    
		}
	}
}
