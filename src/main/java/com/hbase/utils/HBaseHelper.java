
package com.hbase.utils;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;


public class HBaseHelper implements Closeable
{
	private  Configuration configuration = null;
	private Connection connection = null;
	private Admin admin = null;

	public  Configuration getConf() {
		return configuration;
	}

	protected HBaseHelper( Configuration conf ) throws IOException
	{
		this.configuration = conf;
		this.connection = ConnectionFactory.createConnection(configuration);
		this.admin = connection.getAdmin();
	}

	public static HBaseHelper getHelper(Configuration conf)
			throws IOException
	{

		return new HBaseHelper( conf );
	}

	private static void checkAndInitializeKerberos( Configuration config )
	{
		String securityAuthorization = config
				.get("hadoop.security.authorization");
		if (securityAuthorization.equalsIgnoreCase("true")) {
			UserGroupInformation.setConfiguration(config);
		}
	}

	public Connection getConnection() {
		return connection;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public void close() throws IOException
	{
		if(connection != null) {
			connection.close();
		}    
	}

	public boolean existsTable(TableName table)
			throws IOException {
		return admin.tableExists(table);
	}

	public boolean existsTable(String table)
			throws IOException {
		return existsTable(TableName.valueOf(table));
	}

	public void disableTable(String table) throws IOException {
		disableTable(TableName.valueOf(table));
	}

	public void disableTable(TableName table) throws IOException {
		admin.disableTable(table);
	}

	public void dropTable(String table) throws IOException {
		dropTable(TableName.valueOf(table));
	}

	public void dropTable(TableName table) throws IOException {
		if (existsTable(table)) {
			if (admin.isTableEnabled(table)) disableTable(table);
			admin.deleteTable(table);
		}
	}


	public void createTable(String table, String... colfams)
			throws IOException {
		createTable(TableName.valueOf(table), 1, null, colfams);
	}

	public void createTable(TableName table, String... colfams)
			throws IOException {
		createTable(table, 1, null, colfams);
	}

	public void createTable(String table, int maxVersions, String... colfams)
			throws IOException {
		createTable(TableName.valueOf(table), maxVersions, null, colfams);
	}

	public void createTable(TableName table, int maxVersions, String... colfams)
			throws IOException {
		createTable(table, maxVersions, null, colfams);
	}

	public void createTable(String table, byte[][] splitKeys, String... colfams)
			throws IOException {
		createTable(TableName.valueOf(table), 1, splitKeys, colfams);
	}

	public void createTable(TableName table, int maxVersions, byte[][] splitKeys,
			String... colfams)
					throws IOException {
		HTableDescriptor desc = new HTableDescriptor(table);
		for (String cf : colfams) {
			HColumnDescriptor coldef = new HColumnDescriptor(cf);
			coldef.setMaxVersions(maxVersions);
			desc.addFamily(coldef);
		}
		if (splitKeys != null) {
			admin.createTable(desc, splitKeys);
		} else {
			admin.createTable(desc);
		}
	}

}
