package com.bigdata.hbase;

import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class HBaseManager {
    public Connection connection;

    public static void main(String[] args) {
        HBaseManager hBaseManager = new HBaseManager();
        //hBaseManager.createTable("course",new String[]{"cf"});
        //hBaseManager.put("course","003","cf","price","38");
        //hBaseManager.put("course","002","cf","price","28");
        //hBaseManager.put("course","001","cf","price","18");
        //hBaseManager.getResultByRow("course","001");
        //hBaseManager.getResultByScan("course","001","004");
        //hBaseManager.scanAndFilter("course","001","cf","price","20");
        hBaseManager.dropTable("course");
    }

    /**
     * 删除hbase表
     * @param tableName
     */
    public void dropTable(String tableName){
        try {
            Admin admin = connection.getAdmin();
            TableName tableName1 = TableName.valueOf(tableName);
            if(admin.tableExists(tableName1)){
                admin.disableTable(tableName1);
                admin.deleteTable(tableName1);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            closeConnection();
        }
    }

    /**
     * 查询并过滤hbase数据
     * @param tableName
     * @param rowKey
     * @param cf
     * @param col
     * @param val
     */
    public void scanAndFilter(String tableName,String rowKey,String cf,String col,String val){
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(rowKey));

            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(cf),Bytes.toBytes(col),
                    CompareFilter.CompareOp.GREATER,Bytes.toBytes(val)));

            scan.setFilter(filterList);

            ResultScanner rsa = table.getScanner(scan);
            for(Result result:rsa){
                for (Cell cell:result.listCells()){
                    String columnFamily = Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                    String column = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                    Long timestamp = cell.getTimestamp();
                    System.out.println(columnFamily+"=="+column+"=="+value+"=="+timestamp);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            closeTable(table);
            closeConnection();
        }
    }

    /**
     * 根据scan 批量查询hbase
     * @param tableName
     * @param startKey
     * @param endKey
     */
    public void getResultByScan(String tableName,String startKey,String endKey){
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startKey));
            scan.setStopRow(Bytes.toBytes(endKey));
            ResultScanner rsa = table.getScanner(scan);
            for(Result result:rsa){
                for (Cell cell:result.listCells()){
                    String columnFamily = Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                    String column = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                    Long timestamp = cell.getTimestamp();
                    System.out.println(columnFamily+"=="+column+"=="+value+"=="+timestamp);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            closeTable(table);
            closeConnection();
        }
    }

    /**
     * 根据rowkey查询hbase
     * @param tableName
     * @param rowKey
     */
    public void getResultByRow(String tableName,String rowKey){
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get =new Get(rowKey.getBytes());
            Result result = table.get(get);
            for (Cell cell:result.listCells()){
                String columnFamily = Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                Long timestamp = cell.getTimestamp();
                System.out.println(columnFamily+"=="+column+"=="+value+"=="+timestamp);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            closeTable(table);
            closeConnection();
        }
    }

    /**
     * hbase 表插入数据
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     */
    public void put(String tableName,String rowKey,String columnFamily,String column,String value){
        Table table= null;
        try {
            table=connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            closeTable(table);
            //closeConnection();
        }
    }

    /**
     * 关闭表连接
     * @param table
     */
    public void closeTable(Table table){
        if(table!=null){
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 创建hbase表
     * @param name
     * @param cols
     */
    public void createTable(String name,String[] cols){
        try {
            Admin admin=connection.getAdmin();
            TableName tableName = TableName.valueOf(name);
            if(!admin.tableExists(tableName)){
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                for(String col:cols){
                    HColumnDescriptor hColumnDescriptor=new HColumnDescriptor(col);
                    hTableDescriptor.addFamily(hColumnDescriptor);
                }

                admin.createTable(hTableDescriptor);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            closeConnection();
        }
    }

    /**
     * 通过构造方法HBaseManager连接hbase
     */
    public HBaseManager(){
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop1,hadoop2,hadoop3");
        conf.set("hbase.zookeeper.property.clientPort","2181");

        try {
            connection= ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void closeConnection(){
        if(connection!=null && !connection.isClosed()){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
