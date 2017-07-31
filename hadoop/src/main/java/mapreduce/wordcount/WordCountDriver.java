package mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Administrator on 2017/7/23.
 * 相当于yarn集群的客户端
 * 需要封装我们的mr程序的相关运行参数，指定jar包
 * 最后提交给yarn
 */
public class WordCountDriver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
//        conf.set("mapreduce.framework.name","local");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("yarn.resourcemanager.hostname","minimaster1");
        Job job = Job.getInstance(conf);

        //指定本程序jar包所在的本地路径
//        job.setJar("/opt/jar/wordcount/wc.jar");
        job.setJarByClass(WordCountDriver.class);

        //指定本业务job要使用的mapper、Reducer业务类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //指定Map的输出数据K,V类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定最总输出数据KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        job.setCombinerClass(WordCountReducer.class);

        //如梭不设置InputFormat,他默认是TextInputFormat ,切片
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);


        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job输出的文件目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        job.submit();
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }
}