package hadoopRpc.protocol;

/**
 * Created by Administrator on 2017/7/22.
 */
public interface IUserLoginService {

    public static final long versionID = 100L;
    public String login(String name,String passwd);

}