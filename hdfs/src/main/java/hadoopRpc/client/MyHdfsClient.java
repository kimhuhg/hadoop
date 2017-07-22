package hadoopRpc.client;

/**
 * Created by Administrator on 2017/7/22.
 */
import java.net.InetSocketAddress;

import hadoopRpc.protocol.ClientNamenodeProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;


public class MyHdfsClient {

    public static void main(String[] args) throws Exception {
        ClientNamenodeProtocol namenode = RPC.getProxy(ClientNamenodeProtocol.class, 1L,
                new InetSocketAddress("localhost", 8888), new Configuration());
        String metaData = namenode.getMetaData("hdfs.iml");
        System.out.println(metaData);
    }

}