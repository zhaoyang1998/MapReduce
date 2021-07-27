package com.atzy.mapreducecount2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class wordcountdirver {
    public static void main(String[] args) {

        //1 获取job
        Configuration conf = new Configuration();
        Job job = null;
        try {
            job = Job.getInstance(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //2 设置jar包路径
        job.setJarByClass(wordcountdirver.class);

        //3 关联mapper和reducer
        job.setMapperClass(wordcountmapper.class);
        job.setReducerClass(wordcountreducer.class);

        //4 设置mapper的输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5 设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6 设置输入路径和输出路径
        try {
            FileInputFormat.setInputPaths(job, new Path(args[0]));
        } catch (IOException e) {
            e.printStackTrace();
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7 提交job
        boolean result = false;
        try {
            result = job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException|ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.exit(result ? 0 : 1);

    }
}
