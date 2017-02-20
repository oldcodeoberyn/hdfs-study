/*
 * Copyright (c) 2017 Nokia Solutions and Networks. All rights reserved.
 */

package com.nokia.lex.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * @author Lex Li
 * @date 17/02/2017
 */
public class HbaseApp
{
    public static void main( String[] args) throws IOException
    {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource( "hbase-client.xml" );
//        Configuration hconf = HBaseConfiguration.create(ctxt.getConfiguration());
        Connection hbase = ConnectionFactory.createConnection(conf);
//        Table htable = hbase.getTable(getTableName());
//        HBaseAdmin hbase = new HBaseAdmin(conf);
        // Instantiating table descriptor class
        HTableDescriptor tableDescriptor = new
                        HTableDescriptor( TableName.valueOf("emp22"));

        // Adding column families to table descriptor
        tableDescriptor.addFamily(new HColumnDescriptor("personal22"));
        tableDescriptor.addFamily(new HColumnDescriptor("professional22"));

        // Execute the table through admin
        hbase.getAdmin().createTable(tableDescriptor);
        System.out.println(" Table created ");
    }
}
