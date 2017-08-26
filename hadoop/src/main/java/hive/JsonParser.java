package hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;


/**
 * Created by Administrator on 2017/8/20.
 * 解析 json的数据
 */
public class JsonParser extends UDF {

    public String evaluate(String jsonLine) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            MovieRateBean movieRateBean = objectMapper.readValue(jsonLine, MovieRateBean.class);
            return  movieRateBean.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  "";
    }

}
