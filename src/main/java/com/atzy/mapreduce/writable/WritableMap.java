package com.atzy.mapreduce.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.Key;

import java.io.IOException;

public class WritableMap extends Mapper<LongWritable, Text, Text, FlowBean>{
    private FlowBean OutV = new FlowBean();
    private Text OutK = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取一行数据
        String line = value.toString();

        //一行数据格式化
        String[] lines = line.split("\t");

        //设置各个参数
        String phone = lines[1];
        String up = lines[lines.length-3];
        String donw = lines[lines.length-2];

        //设置OUk和ouV
        OutK.set(phone);
        OutV.setUpflow(Long.parseLong(up));
        OutV.setDownflow(Long.parseLong(donw));
        OutV.setSumfllow();

        context.write(OutK,OutV);
    }
}
