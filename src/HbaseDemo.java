import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseDemo {
    private  Configuration conf =null;
    @Before
    private void init() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","spark1:2181,spark2:2181,spark3");
    }
    //插入数据
    @Test
    public void  testPut() throws IOException {
        HTable table = new HTable(conf ,"people");
        Put put = new Put(Bytes.toBytes("rk0001"));
        put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("zhangsanfeng"));
        put.add(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes("300"));
        put.add(Bytes.toBytes("info"),Bytes.toBytes("money"),Bytes.toBytes("30000000"));
        table.put(put);
    }
    //存放大量数据
    @Test
    public void testPutAll() throws IOException {
        //容易造成数据量过大
        HTable table =new HTable(conf ,"people");
        //一开始增16直接24，会逐渐增加
        List<Put> puts = new ArrayList<>(10000);
        for (int i = 1; i <= 1000000 ; i++) {
            Put put = new Put(Bytes.toBytes("rk" + i));
            put.add(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes("" + i));
            puts.add(put);
            if (i % 10000 == 0) {
                table.put(puts);
                puts = new ArrayList<>(10000);
            }
        }
        // 一个一个增加会很满，一万条一个ArrayList
        // table.put(puts);
        table.close();
    }
    //获得
    @Test
    public void testGet() throws IOException {
        HTable table = new HTable(conf , "people");
        Get get = new Get(Bytes.toBytes("kr999999"));
        Result result = table.get(get);
        String toString = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("money")));
        System.out.println(toString);
        table.close();
    }
    //扫描
    @Test
    public void testScan() throws IOException {
        HTable people = new HTable(conf, "people");
        Scan scan = new Scan(Bytes.toBytes("rk29"),Bytes.toBytes("rk30"));
        ResultScanner scanner = people.getScanner(scan);
        for (Result result: scanner
             ) {
            //包含前面不包含后面
            String toString = Bytes.toString(result.getValue(Bytes.toBytes("299999"), Bytes.toBytes("3000000")));
        }
        people.close();
    }
    //删除
    @Test
    public void testDel() throws IOException {
        HTable table = new HTable(conf ,"people");
        Delete delete = new Delete(Bytes.toBytes("rk299999"));
        table.delete(delete);
        table.close();
    }
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","spark1:2181,spark2:2181,spark3");
        //创建一个表
        HBaseAdmin admin = new HBaseAdmin(conf);

        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("peoples"));
        HColumnDescriptor hcd_info = new HColumnDescriptor("info");
        hcd_info.setMaxVersions(3);
        HColumnDescriptor hcd_data = new HColumnDescriptor("data");
        //默认是1
        htd.addFamily(hcd_info);
        htd.addFamily(hcd_data);
        admin.createTable(htd);
        admin.close();
    }
}
