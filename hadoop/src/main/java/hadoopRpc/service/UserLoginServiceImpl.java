package hadoopRpc.service;

import hadoopRpc.protocol.IUserLoginService;

/**
 * Created by Administrator on 2017/7/22.
 */
public class UserLoginServiceImpl implements IUserLoginService {


    public String login(String name, String passwd) {

        return name + "logged in successfully...";
    }


}
