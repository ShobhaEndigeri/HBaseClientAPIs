package com.hbase.examples;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/**
 * 
 * @author Shobha
 * 
 */

public class isHBaseTableEmpty {
	
	public static void main(String args[]) {
		try {
			if(args.length > 0)
				System.out.println(isTableEmpty(args[0])?"Table is Empty":"Table is not Empty");
			else
				System.out.println("Enter table name ");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public static boolean isTableEmpty(String tableName) throws IOException {
		HBaseAdmin admin = null;
		Configuration conf = null;
		Scan scan = new Scan();
		boolean empty = true;
		conf = HBaseConfiguration.create(); 
		admin = new HBaseAdmin( conf );
		if(admin.tableExists( tableName )) {
			HTable table = new HTable( conf, tableName );
			ResultScanner scanner = table.getScanner( scan );
			empty = !(scanner.iterator().hasNext());
			scanner.close();
			
			table.close();
			admin.close();
		}
		return empty;
	}


}
