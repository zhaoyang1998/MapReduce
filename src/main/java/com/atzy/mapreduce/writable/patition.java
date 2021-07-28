package com.atzy.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class patition extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        int patition1;
        String phone = text.toString();
        if ("133".equals(phone)){
            patition1=0;
        }else if("134".equals(phone)){
            patition1=1;
        }else if("135".equals(phone)){
            patition1=2;
        }else {
            patition1=3;
        }

        return patition1;
    }
}
