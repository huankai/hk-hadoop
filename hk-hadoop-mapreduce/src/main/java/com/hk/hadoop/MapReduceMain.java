package com.hk.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 使用本地运行此程序 需要配置hadoop 环境， https://github.com/steveloughran/winutils
 *
 * @author huangkai
 * @date 2018-6-23 11:40
 */
public class MapReduceMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
//        configuration.set("mapreduce.framework.name", "local");// 使用本地模式运行,默认就是本地模式，可查看 mapred-default.xml文件默认配置
        Job job = Job.getInstance(configuration);
        job.setJarByClass(MapReduceMain.class);
        job.setMapperClass(WorldCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        /*
            指定生成输出文件的个数，默认为1 个，文件名为 part-r-00000
            是根据什么规则决定哪个单词存储在哪个文件中的呢?
            默认的分区规则是根据 map 的输出中的 key 进行hash算法，再 % reduceTask
            @see org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
        */
        job.setNumReduceTasks(3);
//        job.setPartitionerClass(); // 设置分区规则

        /*
            combiner:
                每一个 map都会产生大量的本地输出，Combiner的作用就是对map端的输出先做一次合并，以减少 map 和 reduce 之前的数据传输，
                提高网络IO性能，是MapReduce优化手段之一,combiner默认是没有设置的。
                combiner 是 Map 和 Reduce之外的一个组件；
                combiner 组件的父类是 Reducer;
                combiner 和 Reduce 之间的区别在于运行的位置：
                    combiner 是在每个 map 任务所在节点运行，也就是每个 Map 都会有自己的 combiner
                    Reducer 是接收全局所有 Map 的执行结果。
                combiner 对每个 Map 的结果进行局部输出汇总，以减少网络传输量。

           combiner 的实现步骤：
                1、自定义一个combiner 继承于 Reducer ,重写 reduce 方法;
                2、在 job 就设置combiner ，如： job.setCombinerClass(WorldCountReducer.class);
            combiner 能够应用的前提是不影响业务逻辑，（如求和，求中位数则不行，因为求中位数会影响业务逻辑），
            而且 ，combiner 输出的 key 和 value 就是 reducer 输出的 key 和 value.
         */
//        job.setCombinerClass(WorldCountReducer.class);

        job.setReducerClass(WorldCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, "/world/input");//指定输入路径
        FileOutputFormat.setOutputPath(job, new Path("/world/output"));//指定输出路径

//        job.submit(); //只提交程序
        job.waitForCompletion(true);// 提交程序，并监控打印程序执行情况

    }
}
