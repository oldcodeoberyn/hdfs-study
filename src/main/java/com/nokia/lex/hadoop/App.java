package com.nokia.lex.hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.UUID;

/**
 * Hello world!
 *
 */
public class App 
{
    private static final String HADOOP_USER_NAME = "root";
    public static final String MY_PATH = "/mytest";

    public static void main( String[] args )
    {
        System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME);

        Configuration conf = new Configuration(  );
        conf.addResource( "hdfs-client.xml" );
        try
        {
            FileSystem fs = FileSystem.get(conf);

            FSDataOutputStream fsDataOutputStream = fs.create( new Path(  MY_PATH ) );
            try(Writer writer = new OutputStreamWriter(fsDataOutputStream))
            {
                for (int i = 0; i < 100; i++) {
                    writer.write( UUID.randomUUID().toString());
                    writer.write('\n');
                }
            }
            try(BufferedReader reader = new BufferedReader(new InputStreamReader( fs.open( new Path(MY_PATH) )) ))
            {
                for (int i = 100; i > 0; --i) {
                    String line = reader.readLine();
                    if (line == null) {
                        break;
                    }
                    System.out.println(line);
                }
            }

        }
        catch( IOException e )
        {
            e.printStackTrace();
        }
    }
}
