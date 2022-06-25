package com.lyc.bulkload.mr;

import com.lyc.entity.TransferRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MapReduceExtendedCell;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Collections;

/**
 * @ProjectName bankrecord_bulkload
 * @ClassName BankRecordBulkLoad
 * @Description TODO 读取Excel数据生成StoreFile文件
 * @Author lyc
 * @Date 2022/5/19 15:05
 * @Version 1.0
 **/
public class BankRecordBulkLoad {
    public static class BankRecordMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, MapReduceExtendedCell> {
        public final String CF_NAME = "C1";
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //1. 解析文本数据
            String rowText = value.toString();
            TransferRecord tr = TransferRecord.parse(rowText);
            //2. 构建Rowkey
            String rowkey = tr.getId();
           
            //3. 构建键值对
            KeyValue keyValueId = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(CF_NAME), Bytes.toBytes("id"), Bytes.toBytes(tr.getId()));
            KeyValue keyValueCode = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(CF_NAME), Bytes.toBytes("code"), Bytes.toBytes(tr.getCode()));
            KeyValue keyValueRec_account = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(CF_NAME), Bytes.toBytes("rec_account"), Bytes.toBytes(tr.getRec_account()));
            KeyValue keyValueRec_bank_name = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(CF_NAME), Bytes.toBytes("rec_bank_name"), Bytes.toBytes(tr.getRec_bank_name()));
            KeyValue keyValueRec_name = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(CF_NAME), Bytes.toBytes("rec_name"), Bytes.toBytes(tr.getRec_name()));
            KeyValue keyValuePay_account = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(CF_NAME), Bytes.toBytes("pay_account"), Bytes.toBytes(tr.getPay_account()));
            KeyValue keyValuePay_name = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(CF_NAME), Bytes.toBytes("pay_name"), Bytes.toBytes(tr.getPay_name()));
            KeyValue keyValuePay_comments = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(CF_NAME), Bytes.toBytes("pay_comments"), Bytes.toBytes(tr.getPay_comments()));
            KeyValue keyValuePay_channel = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(CF_NAME), Bytes.toBytes("pay_channel"), Bytes.toBytes(tr.getPay_channel()));
            KeyValue keyValuePay_way = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(CF_NAME), Bytes.toBytes("pay_way"), Bytes.toBytes(tr.getPay_way()));
            KeyValue keyValueStatus = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(CF_NAME), Bytes.toBytes("status"), Bytes.toBytes(tr.getStatus()));
            KeyValue keyValueTimestamp = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(CF_NAME), Bytes.toBytes("timestamp"), Bytes.toBytes(tr.getTimestamp()));
            KeyValue keyValueMoney = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(CF_NAME), Bytes.toBytes("money"), Bytes.toBytes(tr.getMoney()));

            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), new MapReduceExtendedCell(keyValueId));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), new MapReduceExtendedCell(keyValueCode));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), new MapReduceExtendedCell(keyValueRec_account));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), new MapReduceExtendedCell(keyValueRec_bank_name));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), new MapReduceExtendedCell(keyValueRec_name));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), new MapReduceExtendedCell(keyValuePay_account));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), new MapReduceExtendedCell(keyValuePay_name));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), new MapReduceExtendedCell(keyValuePay_comments));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), new MapReduceExtendedCell(keyValuePay_channel));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), new MapReduceExtendedCell(keyValuePay_way));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), new MapReduceExtendedCell(keyValueStatus));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), new MapReduceExtendedCell(keyValueTimestamp));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), new MapReduceExtendedCell(keyValueMoney));

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. 获取配置
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(BankRecordBulkLoad.class.getClassLoader().getResource("hbase-site.xml"));
        //2. 获取Hbase连接
        Connection connection = ConnectionFactory.createConnection(conf);
        //3. 获取HTable
        String tableName = "BANK:TRANSFER_RECORD";

        Table table = connection.getTable(TableName.valueOf(tableName));
        //4. 构建MapReduce Job
        Job job = Job.getInstance(conf, "Transfer_BulkLoad_Job");
        //5. 设置Job主类、Mapper、输入
        job.setJarByClass(BankRecordBulkLoad.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(BankRecordMapper.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(MapReduceExtendedCell.class);

        //6. 设置输入、输出
        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop101:8020/bank/input"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop101:8020/bank/output"));

        //7. 配置Hbase StoreFile输出
        //获取Hbase Region的分布情况
        RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(tableName));
        //配置HFile输出
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
        //8. 启动执行MapReduce Job
        if (job.waitForCompletion(true)) {
            System.exit(0);
        }else {
            System.exit(1);
        }
    }
}
