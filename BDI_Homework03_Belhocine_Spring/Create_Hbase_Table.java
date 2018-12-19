// Timo Spring, Hichem Belhocine
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;

import java.io.IOException;
import java.io.InputStreamReader;



public class Create_Hbase_Table {

    static Configuration conf = HBaseConfiguration.create();

    public static void main(String[] args) throws Exception {

        conf.set("hbase.zookeeper.quorum", "diufrm210.unifr.ch");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        Connection connection = ConnectionFactory.createConnection(conf);

        String tableName = "weblogs_bdi18_14";
        TableName webLogsTable = TableName.valueOf(tableName);
        String months = "Months";
        String statistics = "Statistics";
        // Connect to Hbase and create the table
        try {
            Admin hAdmin = connection.getAdmin();
            HTableDescriptor hTableDesc = new HTableDescriptor(webLogsTable);
            hTableDesc.addFamily(new HColumnDescriptor(months));
            hTableDesc.addFamily(new HColumnDescriptor(statistics));
            System.out.println( "Creating Table..." );
            hAdmin.createTable(hTableDesc);
            System.out.println("Table created Successfully...");

        } catch (Exception e) {
            e.printStackTrace();
        }

        Table table = connection.getTable(TableName.valueOf(tableName));
        try {

            // Reading the dataset from HDFS
            Path path = new Path("/bdi_2018/bdi18_14/weblogs_hbase.txt");
            FileSystem fileSystem = FileSystem.get(new Configuration());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));

            // In order to read one line from the bufferedReader, use the readLine() method
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] elements = line.split("\t");
                byte[] rowKey = Bytes.toBytes(elements[0]);
                Put row = new Put(rowKey);
                int visits = 0;
                for(int i = 1 ; i < 13 ; i++ ){
                    if(!elements[i].equals("0")){
                        row.addImmutable(months.getBytes(), Bytes.toBytes(getColumnName(i)), Bytes.toBytes(elements[i]));
                        visits++;
                    }
                }
                if(visits > 20){
                    row.addImmutable(statistics.getBytes(), Bytes.toBytes("Active"), Bytes.toBytes("1"));
                }else{
                    row.addImmutable(statistics.getBytes(), Bytes.toBytes("Active"), Bytes.toBytes("0"));
                }
               table.put(row);
	       System.out.println("Writing row key: " + elements[0]);
            }
	System.out.println("Data successfully written to table!"); 
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static String getColumnName(int monthIndex) {
        switch (monthIndex) {
            case 1:
                return "Jan";
            case 2:
                return "Feb";
            case 3:
                return "Mar";
            case 4:
                return "Apr";
            case 5:
                return "May";
            case 6:
                return "Jun";
            case 7:
                return "Jul";
            case 8:
                return "Aug";
            case 9:
                return "Sep";
            case 10:
                return "Oct";
            case 11:
                return "Nov";
            case 12:
                return "Dec";
        }
        return "";
    }
}

