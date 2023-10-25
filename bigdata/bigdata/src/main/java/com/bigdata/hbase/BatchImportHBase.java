package com.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class BatchImportHBase {
    public static class MyMapper extends Mapper<LongWritable, Text,LongWritable,Text>{
        private Text out = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] records = value.toString().split(",");
            int size = records.length;
            if(size==3){
                String rowKey = records[0]+":"+records[1];
                //rowkey,id,year,temperature
                out.set(rowKey+","+value.toString());
                context.write(key,out);
            }
        }
    }

    public static class MyReducer extends TableReducer<LongWritable,Text, ImmutableBytesWritable>{
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value:values){
                String[] records = value.toString().split(",");
                String rowKey = records[0];
                Put put = new Put(Bytes.toBytes(rowKey));

                put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("id"),Bytes.toBytes(records[1]));
                put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("year"),Bytes.toBytes(records[2]));
                put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("temperature"),Bytes.toBytes(records[3]));

                ImmutableBytesWritable keys = new ImmutableBytesWritable(rowKey.getBytes());

                context.write(keys,put);
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
       Configuration conf =  HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum","hadoop1,hadoop2,hadoop3");
        conf.set("hbase.zookeeper.property.clientPort","2181");

        Job job = Job.getInstance(conf);
        job.setJobName("BatchImportHBase");
        job.setJarByClass(BatchImportHBase.class);
        job.setMapperClass(MyMapper.class);

        TableMapReduceUtil.initTableReducerJob("temperature",MyReducer.class,job,null,null,null,null,false);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));

        job.waitForCompletion(true);
    }
}
