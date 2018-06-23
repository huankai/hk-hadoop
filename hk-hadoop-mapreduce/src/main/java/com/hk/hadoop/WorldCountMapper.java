package com.hk.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <p>
 * 统计单词个数 Mapper
 * </p>
 * <p>
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> <br/>
 * KEYIN: 表示mapper 阶段数据输入的 key 类型，在默认读取数据的组件下，叫 InputFormat，它的行为是一行行的读取待处理的数据，KEYIN就是每一行的起始偏移量；<br/>
 * VALUEIN: 表示mapper 阶段数据输入的 value 类型，在默认读取数据的组件下，VALUEIN就是每一行的内容，因此数据类型为String ； <br/>
 * KEYOUT: 表示mapper数据输出时的 key 的数据类型，在本案例中，输出的key是单词，所以数据类型为 String ；<br/>
 * VALUEOUT:表示mapper数据输出时的 value 的数据类型，在本案例中，输出的 value 是单词，所以数据类型为 Integer；<br/>
 * </p>
 * <p>
 * <p>
 * 以上所有的数据类型(Long/Integer/String) 都是JDK自带的数据类型，的序列化时，效率低下，因此，hadoop封装了一套自己的数据类型: <br/>
 * String  ----> Text <br/>
 * Long  ----> LongWritable  <br/>
 * Integer  ----> IntWritable <br/>
 * null  ----> NullWritable
 * </p>
 *
 * @author huangkai
 * @date 2018-6-23 11:42
 */
public class WorldCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    /**
     * 此方法就是 Map 阶段具体的业务逻辑实现
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            //把 map 阶段处理的数据通过 context 发送出去，作为reduce的输入数据.
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
