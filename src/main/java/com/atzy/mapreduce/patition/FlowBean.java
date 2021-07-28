package com.atzy.mapreduce.patition;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
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
        this.sumfllow=in.readLong();

        this.downflow=in.readLong();
        this.upflow=in.readLong();
    }

    @Override
    public String toString() {
        return  upflow +"\t"+downflow+"\t"+sumfllow;
    }
}
