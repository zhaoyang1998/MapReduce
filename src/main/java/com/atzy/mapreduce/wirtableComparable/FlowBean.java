package com.atzy.mapreduce.wirtableComparable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {
    private long upflow;
    private long downflow;
    private long sumfllow;
    public long getUpflow() {
        return upflow;
    }

    public void setUpflow(long upflow) {
        this.upflow = upflow;
    }

    public long getDownflow() {
        return downflow;
    }

    public void setDownflow(long downflow) {
        this.downflow = downflow;
    }

    public long getSumfllow() {
        return sumfllow;
    }

    public void setSumfllow() {
        this.sumfllow = this.downflow+this.upflow;
    }



    public FlowBean() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upflow);
        out.writeLong(downflow);
        out.writeLong(sumfllow);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upflow=in.readLong();
        this.downflow=in.readLong();
        this.sumfllow=in.readLong();


    }

    @Override
    public String toString() {
        return  upflow +"\t"+downflow+"\t"+sumfllow;
    }

    @Override
    public int compareTo(FlowBean o) {

        //总流量的倒叙排序
        if (this.sumfllow >o.sumfllow){
            return -1;
        }else if (this.sumfllow<o.sumfllow){
            return 1;
        }else{
            if (this.upflow>o.upflow){
                return 1;
            }else if (this.upflow<o.upflow){
                return -1;
            }else{
                return 0;
            }

        }
    }
}
