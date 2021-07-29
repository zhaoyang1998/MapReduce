package com.atguigu.mapreduce.outputformat;
import com.atzy.mapreduce.wordcountformat.LogMap;
import com.atzy.mapreduce.wordcountformat.LogOutputformat;
import com.atzy.mapreduce.wordcountformat.LogReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
public  class logDrver {
    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(logDrver.class);
        job.setMapperClass(LogMap.class);
        job.setReducerClass(LogReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置自定义的 outputformat
        job.setOutputFormatClass(LogOutputformat.class);
        FileInputFormat.setInputPaths(job, new Path("/Users/zhaoyang/Desktop/hadoop学习/inputou"));
        //虽然我们自定义了 outputformat，但是因为我们的 outputformat 继承自 fileoutputformat
        // 而 fileoutputformat 要输出一个_SUCCESS 文件，所以在这还得指定一个输出目录
        FileOutputFormat.setOutputPath(job, new Path("/Users/zhaoyang/Desktop/hadoop学习/ou"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}