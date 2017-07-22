package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by liyang on 2017/7/2.
 * 客户端去操作hdfs时，是有一个用户身份的
 * 默认情况下，hdfs客户端api会从jvm中获取一个参数来作为自己的用户身份：-DHADOOP_USER_NAME=hadoop
 * <p>
 * 也可以在构造客户端fs对象时，通过参数传递进去
 *
 * @author
 */

public class HdfsClientDemo1 {
    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void init() throws Exception {
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://miniMaster1:9000");
        conf.set("dfs.replaction", "2");
        System.out.println(conf.get("dfs.replaction"));
        //拿到一个文件系统操作的客户端实例对象
        /*fs = FileSystem.get(conf);*/
        //可以直接传入 uri和用户身份
        fs = FileSystem.get(new URI("hdfs://miniMaster1:9000"), conf, "root");
        //最后一个参数为用户名

    }

    @Test
    public void testUpload() throws Exception {
        Thread.sleep(2000);
        fs.copyFromLocalFile(new Path("H:\\036_2016传智大数据第3期实战培训完整版 压缩\\[www.17zixueba.com]资料\\[www.17zixueba.com]day06--hadoop\\[www.17zixueba.com]day06\\shizhan_03_hadoop\\src\\cn\\itcast\\bigdata\\hdfs\\HdfsClientDemo.java"), new Path("/HdfsClientDemo.java.copy1"));
        fs.close();
    }

    @Test
    public void testDownload() throws Exception {

        fs.copyToLocalFile(new Path("/HdfsClientDemo.java.copy1"), new Path("h:/"));
        fs.close();
    }

    @Test
    public void testConf() {
        Iterator<Map.Entry<String, String>> iterator = conf.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            System.out.println(entry.getValue() + "--" + entry.getValue());//conf加载的内容
        }
    }

    /**
     * 创建目录
     */
    @Test
    public void makdirTest() throws Exception {
        boolean mkdirs = fs.mkdirs(new Path("/aaa/bbb"));
        System.out.println(mkdirs);
    }

    /**
     * 删除
     */
    @Test
    public void deleteTest() throws Exception {
        boolean delete = fs.delete(new Path("/HdfsClientDemo.java.copy1"), true);//true， 递归删除
        System.out.println(delete);
    }

    @Test
    public void listTest() throws Exception {
        FileStatus[] listStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatuses){
            System.err.println(fileStatus.getPath()+"================="+fileStatus.toString());
        }

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()){
            //会递归找到所有的文件
            LocatedFileStatus next = listFiles.next();
            String name = next.getPath().getName();
            Path path = next.getPath();System.out.println(name + "---" + path.toString());

        }
    }


    @Test
    public void testListFiles() throws Exception {

        // 思考：为什么返回迭代器，而不是List之类的容器
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocations) {
                System.out.println("block-length:" + bl.getLength() + "--" + "block-offset:" + bl.getOffset());
                String[] hosts = bl.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("--------------为angelababy打印的分割线--------------");
        }
    }

    /**
     * 查看文件及文件夹信息
     *
     */
    @Test
    public void testListAll() throws Exception {

        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        String flag = "d--             ";
        for (FileStatus fstatus : listStatus) {
            if (fstatus.isFile())  flag = "f--         ";
            System.out.println(flag + fstatus.getPath().getName());
        }
    }

}
