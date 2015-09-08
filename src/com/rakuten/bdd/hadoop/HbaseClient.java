package com.rakuten.bdd.hadoop;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

/**
 * Hbase client example
 * 
 * @author Neil.Tu
 * 
 */
public class HbaseClient {
	private final static Log LOG = LogFactory.getLog(HbaseClient.class);
	private Configuration configuration = null;
	private HBaseAdmin hbaseAdmin = null;

	public HbaseClient() {

		configuration = HBaseConfiguration.create();
		configuration.addResource(new Path("/home/tuneil01/hbase/conf/hbase-site.xml"));
		try {
			hbaseAdmin = new HBaseAdmin(configuration);
		} catch (Exception e) {
			LOG.error(e);
		}
	}

	public void createNamespace(String nameSpace) throws IOException {
		hbaseAdmin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
		System.out.println("Create Namespace " + nameSpace + " successfully..");
	}

	public void createHTable(String tableName, String[] columnFamily) throws IOException {
		if (hbaseAdmin.tableExists(tableName)) {
			System.out.println(tableName + "exist");
		}

		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

		for (String colunm : columnFamily) {
			tableDescriptor.addFamily(new HColumnDescriptor(colunm));
		}

		hbaseAdmin.createTable(tableDescriptor);
		System.out.println("Create table " + tableName + " successfully..");
	}

	public void deleteHTable(String tableName) throws IOException {
		hbaseAdmin.deleteTable(tableName);
	}

	public void disableHTable(String tableName) throws IOException {
		hbaseAdmin.disableTable(tableName);
	}

	public HTable findTable(String tableName) throws IOException {
		HTable table = new HTable(configuration, tableName);
		return table;
	}

	public void put(HTable htable, String rowName, String family, String qualifier, String value) throws IOException {
		Put put = new Put(toBytes(rowName));
		put.add(toBytes(family), toBytes(qualifier), toBytes(value));
		htable.put(put);
		htable.flushCommits();
	}

	public void put(String tableName, String rowName, String family, String qualifier, String value) throws IOException {
		HTable table = findTable(tableName);
		this.put(table, rowName, family, qualifier, value);
		System.out.println("Put " + rowName + " into " + tableName + " successfully..");
	}

	public void delete(HTable htable, String rowName, String family, String qualifier) throws IOException {
		Delete delete = new Delete(toBytes(rowName));
		delete.deleteColumn(toBytes(family), toBytes(qualifier));
		htable.delete(delete);
		htable.flushCommits();
	}

	public void delete(String tableName, String rowName, String family, String qualifier) throws IOException {
		HTable table = findTable(tableName);
		this.delete(table, rowName, family, qualifier);

	}

	public static void main(String[] args) throws IOException {
		HbaseClient hbase = new HbaseClient();

		if (args[0].equals("createNamespace")) {
			hbase.createNamespace("neil_ns");

		} else if (args[0].equals("createHTable")) {
			String[] column = { "grade", "course" };
			hbase.createHTable("neil_ns:students", column);

		} else if (args[0].equals("put")) {
			hbase.put("neil_ns:students", "Tom", "grade", "", "4");
			hbase.put("neil_ns:students", "Tom", "course", "math", "90");
			hbase.put("neil_ns:students", "Jim", "grade", "", "6");
			hbase.put("neil_ns:students", "Jim", "course", "art", "80");

		} else if (args[0].equals("delete")) {
			hbase.delete("neil_ns:students", "Jim", "course", "art");

		} else if (args[0].equals("deleteHTable")) {
			hbase.disableHTable("neil_ns:students");
			hbase.deleteHTable("neil_ns:students");		
		}
	}
}
