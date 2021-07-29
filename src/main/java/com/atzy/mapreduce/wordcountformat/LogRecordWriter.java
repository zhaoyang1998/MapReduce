package com.atzy.mapreduce.wordcountformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream atguigu;
    private  FSDataOutputStream other;
    public LogRecordWriter(TaskAttemptContext job) {
        try {
            FileSystem fs =FileSystem.get(job.getConfiguration());

            atguigu = fs.create(new Path("/Users/zhaoyang/Desktop/hadoop学习/ou/atguigu.log"));
            other = fs.create(new Path("/Users/zhaoyang/Desktop/hadoop学习/ou/other.log"));
        }catch (IOException e){
            e.printStackTrace();
        }


    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        //具体写
        String log = key.toString();
        if (log.contains("atguigu")){
            atguigu.writeBytes(log+"\n");

        }else{
            other.writeBytes(log+"\n");
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguigu);
        IOUtils.closeStream(other);
    }
}
