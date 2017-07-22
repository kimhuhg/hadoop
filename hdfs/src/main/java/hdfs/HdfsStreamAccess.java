package hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Created by Administrator on 2017/7/22.
 */
public class HdfsStreamAccess {

    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        fs = FileSystem.get(conf);
        fs = FileSystem.get(new URI("hdfs://minimaster1:9000"), conf, "root");
    }

    /**
     * 通过流的方式上传文件到hdfs
     * @throws Exception
     */
    @Test
    public void testUpload() throws Exception {

        FSDataOutputStream outputStream = fs.create(new Path("/hdfs.iml"), true);
        FileInputStream inputStream = new FileInputStream("hdfs.iml");

        IOUtils.copy(inputStream, outputStream);

    }

    @Test
    public void testDownLoadFileToLocal() throws IllegalArgumentException, IOException{

        //先获取一个文件的输入流----针对hdfs上的
        FSDataInputStream in = fs.open(new Path("/hdfs.iml"));

        //再构造一个文件的输出流----针对本地的
        FileOutputStream out = new FileOutputStream(new File("c:/hdfs.iml"));

        //再将输入流中数据传输到输出流
        org.apache.hadoop.io.IOUtils.copyBytes(in, out, 4096);


    }


    /**
     * hdfs支持随机定位进行文件读取，而且可以方便地读取指定长度
     * 用于上层分布式运算框架并发处理数据
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void testRandomAccess() throws IllegalArgumentException, IOException{
        //先获取一个文件的输入流----针对hdfs上的
        FSDataInputStream in = fs.open(new Path("/hdfs.iml"));


        //可以将流的起始偏移量进行自定义
        in.seek(22);

        //再构造一个文件的输出流----针对本地的
        FileOutputStream out = new FileOutputStream(new File("c:/hdfs.iml"));

        org.apache.hadoop.io.IOUtils.copyBytes(in,out,19L,true);

    }



    /**
     * 显示hdfs上文件的内容
     * @throws IOException
     * @throws IllegalArgumentException
     */
    @Test
    public void testCat() throws IllegalArgumentException, IOException{

        FSDataInputStream in = fs.open(new Path("/hdfs.iml"));

        org.apache.hadoop.io.IOUtils.copyBytes(in, System.out, 1024);
    }
}


