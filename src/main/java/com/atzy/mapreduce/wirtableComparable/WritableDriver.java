package com.atzy.mapreduce.wirtableComparable;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WritableDriver {
    public static void main(String[] args) {
        //1.获取job
        Configuration conf = new Configuration();
        Job job= null;
        try {
            job = Job.getInstance(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //2.设置jar包
        job.setJarByClass(WritableDriver.class);

        //3 管理hadoop的mao和reduece
        job.setMapperClass(WritableMap.class);
        job.setReducerClass(WritableReduce.class);

        //4 设置map的输出key和value
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //5 设置最终的输出key和value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        //6 设置数据的输入和输出路径
        try {
            FileInputFormat.setInputPaths(job,new Path("/Users/zhaoyang/Desktop/hadoop学习/outwrite"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        FileOutputFormat.setOutputPath(job,new Path("/Users/zhaoyang/Desktop/hadoop学习/outwrite66"));


        //7提交joj
        boolean result= false;
        try {
            result = job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.exit(result?0:1);
    }

}
