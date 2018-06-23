package com.hk.hadoop.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * <p>
 * Writable 接口是 Hadoop 中的序列化接口，与 JDK 中的 Serializable 类似，
 * 但 JDK 中的序列化是一个重量级的序列化框架(相对于 Hadoop中来讲)，JDK 中一个对象序列化时会附带很多额外的信息
 * （如各种校验信息、header、继承体系等），不便于在网络中高效传输，
 * 所以 Hadoop定义了一套自己的 序列化机制(Writable)，只需要实现 此接口即可
 * </p>
 *
 * @author huangkai
 * @date 2018-6-23 19:41
 */
public class FlowBean implements Writable {


    private long upFlow;

    private long downFlow;

    public FlowBean() {
    }

    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
    }

    /**
     * 序列化
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
    }

    /**
     * 反序列化，反序列化时要注意顺序，
     * 先序列化什么属性，反序列化就会先拿到什么属性
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
    }
}
