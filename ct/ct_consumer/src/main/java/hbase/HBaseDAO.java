package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import utils.HBaseUtil;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Create by fengqijie
 * 2019/3/2 16:25
 */
public class HBaseDAO {

    private int regions;
    private String namespace;
    private String tableName;
    public static final Configuration conf;
    private HTableInterface hTable;
    private HConnection connection;
    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");

    private List<Put> cacheList = new ArrayList<>();
    static {
        conf = HBaseConfiguration.create();
    }

    /**
     *   hbase.calllog.regions=6
         hbase.calllog.namespace=ns_ct
         hbase.calllog.tablename=ns_ct:calllog
     */
    public HBaseDAO() {
        this.regions = Integer.parseInt(PropertiesUtil.getProperty("hbase.calllog.regions"));
        this.namespace = PropertiesUtil.getProperty("hbase.calllog.namespace");
        this.tableName = PropertiesUtil.getProperty("hbase.calllog.tablename");

        try {
            if (!HBaseUtil.isExistTable(conf, tableName)) {
                HBaseUtil.initNamespace(conf, namespace);
                HBaseUtil.createTable(conf, tableName, regions,"f1", "f2");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * ori数据样式： 18576581848,17269452013,2017-08-14 13:38:31,1761
     * rowkey样式：01_18576581848_20170814133831_17269452013_1_1761
     * HBase表的列：call1  call2   build_time   build_time_ts   flag   duration
     * @param ori
     */
    public void put(String ori) {
        try {
            if (cacheList.size() == 0) {
                connection = HConnectionManager.createConnection(conf);
                hTable = connection.getTable(TableName.valueOf(tableName));
                // setAutoFlush(autoFlush, clearBufferOnFail)
                // autoFlush 为false, 则当put填满客户端写缓存时，才向HBase服务端发起请求，
                // 而不是有一条put就执行一次更新, autoFlush默认为true
                // clearBufferOnFail 为true，则不会重复提交响应错误的数据。clearBufferOnFail默认是true的。
                hTable.setAutoFlush(false, false);
                hTable.setWriteBufferSize(2 * 1024 * 1024);
            }

            String[] splitOri = ori.split(",");
            String caller = splitOri[0];  // 主叫
            String callee = splitOri[1];  // 被叫
            String buildTime = splitOri[2]; // 开始通话时间
            String duration = splitOri[3];  // 通话时长
            String regionCode = HBaseUtil.genRegionCode(caller, buildTime, regions);  // 分区号

            String buildTimeReplace = sdf2.format(sdf1.parse(buildTime));  // yyyyMMddHHmmss
            String buildTimeTs = String.valueOf(sdf1.parse(buildTime).getTime()); //时间戳字段

            // 生成rowkey    "1": 主叫
            String rowkey = HBaseUtil.genRowKey(regionCode, caller, buildTimeReplace, callee, "1", duration);

            // 向表中插入该条数据
            Put put = new Put(Bytes.toBytes(rowkey));
            // HBase表的列：call1  call2   build_time   build_time_ts   flag   duration
            put.add(Bytes.toBytes("f1"), Bytes.toBytes("call1"), Bytes.toBytes(caller));
            put.add(Bytes.toBytes("f1"), Bytes.toBytes("call2"), Bytes.toBytes(callee));
            put.add(Bytes.toBytes("f1"), Bytes.toBytes("build_time"), Bytes.toBytes(buildTime));
            put.add(Bytes.toBytes("f1"), Bytes.toBytes("build_time_ts"), Bytes.toBytes(buildTimeTs));
            put.add(Bytes.toBytes("f1"), Bytes.toBytes("flag"), Bytes.toBytes("1"));
            put.add(Bytes.toBytes("f1"), Bytes.toBytes("duration"), Bytes.toBytes(duration));



            System.out.println("caller: " + caller + ", callee: " + callee + ", buildTime: " +
            buildTime + ", buildTimeTs: " + buildTimeTs + ", duration: " + duration);

            cacheList.add(put);

            if (cacheList.size() >= 30) {
                hTable.put(cacheList);
                hTable.flushCommits();

                hTable.close();
                cacheList.clear();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }


}
