package com.atzy.mapreduce.wirtableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WritableMap extends Mapper<LongWritable, Text, FlowBean, Text>{
    private FlowBean outK = new FlowBean();
    private Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取一行
        String line = value.toString();

        //切割
        String[] split = line.split("\t");

        //封装
        outV.set(split[0]);
        outK.setUpflow(Long.parseLong(split[1]));
        outK.setDownflow(Long.parseLong(split[2]));
        outK.setSumfllow();

        context.write(outK,outV);
    }
}
