// Timo Spring, Hichem Belhocine

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.NavigableMap;


public class Hbase_Queries {

    static Configuration conf = HBaseConfiguration.create();

    public static void main(String[] args) throws Exception {

        conf.set("hbase.zookeeper.quorum", "diufrm210.unifr.ch");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("weblogs_bdi18_14"));

        //Retrieve only the contents of the Columns: Jan and Feb from the row key: 06.305.307.336|2012
        byte[] rowKey = Bytes.toBytes("06.305.307.336|2012");
        Get row = new Get(rowKey);
        row.addColumn(Bytes.toBytes("Months"), Bytes.toBytes("Jan"));
        row.addColumn(Bytes.toBytes("Months"), Bytes.toBytes("Feb"));
        Result res = table.get(row);
        String jan = new String(res.getValue(Bytes.toBytes("Months"), Bytes.toBytes("Jan")));
        String feb = new String(res.getValue(Bytes.toBytes("Months"), Bytes.toBytes("Feb")));

	String r = new String(rowKey); 
        System.out.println("Values to Row Key: " + r);
        System.out.println("January: " + jan);
        System.out.println("February: " + feb);

        //Create a new ip and year, and fill in the table with the same values as the row with key: 01.660.70.74|2012
        byte[] key = Bytes.toBytes("01.660.70.74|2012");
        System.out.println("Copying: 01.660.70.74|2012");
        Get origin  = new Get(key);
        Result rowToCopy = table.get(origin);

        Put newRow = new Put(Bytes.toBytes("192.168.20.1|2018"));

        NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierMap = rowToCopy.getNoVersionMap();
        for (byte[] familyBytes : familyQualifierMap.keySet()) {
            NavigableMap<byte[], byte[]> qualifierMap = familyQualifierMap.get(familyBytes);

            for (byte[] qualifier : qualifierMap.keySet()) {
                newRow.add(familyBytes, qualifier, qualifierMap.get(qualifier));
            }
        }
        table.put(newRow);
        System.out.println("Successfully copied to new row with key 192.168.20.1|2018");

        //Delete the row with key: 88.88.324.601|2012
        System.out.println("Delete row with key 88.88.324.601|2012");
        Delete del = new Delete(Bytes.toBytes("88.88.324.601|2012"));
        table.delete(del);

        table.close();
        connection.close();
    }

}

