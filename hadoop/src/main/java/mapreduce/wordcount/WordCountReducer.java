package mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by Administrator on 2017/7/23.
 * <p>
 * *KEYIN,VALUEIN 对应的是Map输出的KEYOUT，VALUEOUT类型对应
 * <p>
 * KEYOUT: 是单词，String, 用Text
 * ValueOUT：是单词总次数，Integer，同上，用IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * <a,1>
     * <b,1><b,1><b,1>
     * <c,1><c,1>
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);
        int count = 0;
//        Iterator<IntWritable> iterator = values.iterator();
//        while (iterator.hasNext()){
//            co
//        }
        for (IntWritable value : values) {
            count += value.get();
        }
        context.write(key,new IntWritable(count));

    }
}
