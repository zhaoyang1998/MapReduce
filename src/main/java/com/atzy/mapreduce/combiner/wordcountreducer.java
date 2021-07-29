package com.atzy.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN, reduce阶段输入的key的类型：Text
 * VALUEIN, reduce阶段输入的value类型 一般是 intwritabke
 * KEYOUT,reduce阶段输出的key类型：Text
 * VALUEOUT reduce阶段输出的value类型：intwrinteable
 */
public class wordcountreducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        //累加
        for (IntWritable value : values) {
            sum += value.get();
        }
        IntWritable outV=new IntWritable();
        outV.set(sum);

        //写出
        context.write(key,outV);
    }
}
