package utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.TreeSet;


/**
 * Create by fengqijie
 * 2019/3/2 16:28
 */
public class HBaseUtil {

    /**
     * 判断表是否存在
     * @param conf
     * @param tableName
     * @return
     */
    public static boolean isExistTable(Configuration conf, String tableName) throws IOException {

        // hbase api 1.3.x
//        Connection connection = ConnectionFactory.createConnection(conf);
//        Admin admin = connection.getAdmin();
//        boolean result = admin.tableExists(TableName.valueOf(tableName));
//
//        admin.close();
//        connection.close();


//        Configuration conf = HBaseConfiguration.create();
        HConnection connection = HConnectionManager.createConnection(conf);
        HBaseAdmin hBaseAdmin = new HBaseAdmin(connection);
        boolean result = hBaseAdmin.tableExists(TableName.valueOf(tableName));
        System.out.println("result=" + result);
        hBaseAdmin.close();
        connection.close();
        return result;
    }

    /**
     * 初始化命名空间
     * @param conf
     * @param namespace
     */
    public static void initNamespace(Configuration conf, String namespace) throws IOException {

        HConnection connection = HConnectionManager.createConnection(conf);
        HBaseAdmin hBaseAdmin = new HBaseAdmin(connection);

        NamespaceDescriptor nd = NamespaceDescriptor
                .create(namespace)
                .addConfiguration("CREATE_TIME", String.valueOf(System.currentTimeMillis()))
                .addConfiguration("AUTHOR", "JinJi")
                .build();

        hBaseAdmin.createNamespace(nd);
        hBaseAdmin.close();
        connection.close();
    }

    /**
     * 创建表
     * @param conf
     * @param tableName
     * @param regions
     * @param columnFamily
     * @throws IOException
     */
    public static void createTable(Configuration conf, String tableName, int regions, String...  columnFamily) throws IOException {

        HConnection connection = HConnectionManager.createConnection(conf);
        HBaseAdmin hBaseAdmin = new HBaseAdmin(connection);

        if (isExistTable(conf, tableName)) {
            return;
        }

        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : columnFamily) {
            htd.addFamily(new HColumnDescriptor(cf));
        }

        htd.addCoprocessor("hbase.CalleeWriteObserver");
        hBaseAdmin.createTable(htd, genSplitKeys(regions));
        hBaseAdmin.close();
        connection.close();
    }

    private static byte[][] genSplitKeys(int regions){
        // 定义一个存放分区键的数组
        String[] keys = new String[regions];
        // 一般一个region维护1G到10G
        // 目前推算，region个数不会超过2位数，所以region分区键格式化为两位数字所代表的字符串
        DecimalFormat df = new DecimalFormat("00");
        for(int i = 0; i < regions; i ++){
            keys[i] = df.format(i) + "|";
        }
//        System.out.println(Arrays.toString(keys));
        byte[][] splitKeys = new byte[regions][];
        // 生成byte[][]类型的分区键的时候，一定要保证分区键是有序的
        TreeSet<byte[]> treeSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for(int i = 0; i < regions; i++){
            treeSet.add(Bytes.toBytes(keys[i]));
        }

        Iterator<byte[]> splitKeysIterator = treeSet.iterator();
        int index = 0;
        while(splitKeysIterator.hasNext()){
            byte[] b = splitKeysIterator.next();
            splitKeys[index ++] = b;
        }
        return splitKeys;
    }



    /**
     * 生成rowkey
     * regionCode_call1_buildTime_call2_flag_duration
     * @return
     */
    public static String genRowKey(String regionCode, String call1, String buildTime, String call2, String flag, String duration){
        String separator = "_";
        StringBuilder sb = new StringBuilder();
        sb.append(regionCode + separator)
                .append(call1 + separator)
                .append(buildTime + separator)
                .append(call2 + separator)
                .append(flag + separator)
                .append(duration);
        return sb.toString();
    }

    /**
     * 手机号：15837312345
     * 通话建立时间：2017-01-10 11:20:30 -> 20170110112030
     * @param call1
     * @param buildTime
     * @param regions
     * @return
     */
    public static String genRegionCode(String call1, String buildTime, int regions){
        int len = call1.length();
        //取出后4位号码
        String lastPhone = call1.substring(len - 4);
        //取出年月
        String ym = buildTime
                .replaceAll("-", "")
                .replaceAll(":", "")
                .replaceAll(" ", "")
                .substring(0, 6);
        //离散操作1
        Integer x = Integer.valueOf(lastPhone) ^ Integer.valueOf(ym);
        int a = 10;
        int b = 20;
        a = a ^ b;
        b = a ^ b;
        a = a ^ b;
        //离散操作2
        int y = x.hashCode();
        //生成分区号
        int regionCode = y % regions;
        //格式化分区号
        DecimalFormat df = new DecimalFormat("00");
        return df.format(regionCode);
    }


}
