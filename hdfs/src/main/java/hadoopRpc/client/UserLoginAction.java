package hadoopRpc.client;

/**
 * Created by Administrator on 2017/7/22.
 */
import java.net.InetSocketAddress;

import hadoopRpc.protocol.IUserLoginService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class UserLoginAction {
    public static void main(String[] args) throws Exception {
        IUserLoginService userLoginService = RPC.getProxy(IUserLoginService.class, 100L, new InetSocketAddress("localhost", 9999), new Configuration());
        String login = userLoginService.login("angelababy", "1314520");
        System.out.println(login);

    }
}