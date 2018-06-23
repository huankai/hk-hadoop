package com.hk.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * <p>
 * reduce 阶段
 * </p>
 * <p>
 * KEYIN: 对应 mapper阶段的keyout类型; <br/>
 * VALUEIN: 对应mapper阶段的 valueout类型; <br/>
 * KEYOUT: reduce 阶段输出的key 类型，本案例是统计单词的个数，key就是单词; <br/>
 * VALUEOUT: reduce 阶段输出的 value 类型 ，就是单词的个数. <br/>
 * </p>
 *
 * @author huangkai
 * @date 2018-6-23 12:34
 */
public class WorldCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * <p>
     * reduce 阶段接收 mapper　阶段处理的数据后，会按照　key 的字典进行排序
     * </p>
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = (int) StreamSupport.stream(values.spliterator(), false)
                .collect(Collectors.summarizingInt(IntWritable::get)).getSum();
        context.write(key, new IntWritable(count));
    }
}
