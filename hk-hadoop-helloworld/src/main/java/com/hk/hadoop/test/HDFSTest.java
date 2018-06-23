package com.hk.hadoop.test;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * @author huangkai
 * @date 2018-6-23 9:12
 */
public class HDFSTest {

    private FileSystem fileSystem;

    @Before
    public void init() throws IOException {
        Configuration conf = new Configuration();
        //配置使用的是 HDFS 文件系统，如果不指定，会使用 core-default.xml（在hadoop-common jar包中） 中的默认配置
        conf.set("fs.defaultFS", "hdfs://192.168.64.1:9000");
        //配置 HADOOP 的用户名，Linux 上的每个文件或目录都有可读、可写、可执行的权限，这里需要保证上传的文件需要有可写的权限
        conf.set("HADOOP_USER_NAME", "root");

        fileSystem = FileSystem.get(conf);
//        也可以使用如下方式来获取 FileSystem对象，不需要在 conf中配置 fs.defaultFS 和 HADOOP_USER_NAME这两个参数
//        FileSystem.get(new URI("hdfs://192.168.64.1:9000"), conf, "root");
    }

    /**
     * Create Dir
     */
    public void createTest() throws IOException {
        //在根目录创建 JavaHelloDir 目录
        fileSystem.create(new Path("JavaHelloDir"), false);
    }

    /**
     * Create Dir
     *
     * @throws IOException
     */
    public void createDirTest() throws IOException {
        fileSystem.mkdirs(new Path("/test"));
    }

    /**
     * 删除目录
     *
     * @throws IOException
     */
    public void deleteDirTest() throws IOException {
        //删除目录，第二个参数表示是否递归删除子级
        fileSystem.delete(new Path("/test"), true);
    }

    /**
     * 重命名目录
     *
     * @throws IOException
     */
    public void renameDirTest() throws IOException {
        fileSystem.rename(new Path("/test"), new Path("/test2"));
    }

    /**
     * Upload File
     *
     * @throws IOException
     */
    public void uploadFileTest() throws IOException {
//       将本地 D盘 a.txt文件上传到 JavaHelloDir 目录中
        fileSystem.copyFromLocalFile(new Path("D:/a.txt"), new Path("/JavaHelloDir"));
    }

    /**
     * Down File
     *
     * @throws IOException
     */
    public void downFileTest() throws IOException {
        //将 /JavaHelloDir/a.txt 下载到D盘 b.txt,在 windows 平台下，需要配置hadoop环境，查看 ： https://blog.csdn.net/woshixuye/article/details/53537519
        fileSystem.copyToLocalFile(new Path("/JavaHelloDir/a.txt"), new Path("D:/b.txt"));
    }

    /**
     *获取文件信息
     * @throws IOException
     */
    public void fileInfoTest() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/"), true);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            System.out.println(file.getBlockSize());
            System.out.println(file.getAccessTime());
            System.out.println(file.getPath().getName());
            System.out.println(file.getPermission());
            System.out.println(file.getLen());
            for (BlockLocation blockLocation : file.getBlockLocations()) {
                System.out.println(blockLocation.getLength());
                System.out.println(blockLocation.getOffset() );
            }

        }
    }

    /**
     * 使用 stream 操作
     *
     * @throws IOException
     */
    public void streamOperatorTest() throws IOException {
        FSDataOutputStream outputStream = fileSystem.create(new Path("/a.txt"), true);
        FileInputStream inputStream = new FileInputStream("D:/a.txt");
        IOUtils.copy(inputStream, outputStream);
    }

    @After
    public void after() throws IOException {
        if (null != fileSystem) {
            fileSystem.close();
        }
    }

}
