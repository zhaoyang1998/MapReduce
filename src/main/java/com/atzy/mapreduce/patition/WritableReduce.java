package com.atzy.mapreduce.patition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WritableReduce extends Reducer<Text, FlowBean,Text, FlowBean> {
    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long totalup = 0;
        long totaldonw = 0 ;

        //1遍历集合累加
        for (FlowBean value : values) {
            totalup+=value.getUpflow();
            totaldonw+=value.getDownflow();
        }

        //2.封装输出的outk和outv
        outV.setUpflow(totalup);
        outV.setDownflow(totaldonw);
        outV.setSumfllow();

        context.write(key,outV);

    }
}
