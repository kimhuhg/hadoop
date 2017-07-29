package mapreduce.flowcount;

import mapreduce.wordcount.WordCountDriver;
import mapreduce.wordcount.WordCountMapper;
import mapreduce.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCount {
    static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取一行内容转化成数据
            String line = value.toString();
            //切分字段
            String[] fields = line.split("\t");
            //取出手机号
            String phoneNumber = fields[1];
            //取出上行下行流量
            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long dFlow = Long.parseLong(fields[fields.length - 2]);
            context.write(new Text(phoneNumber), new FlowBean(upFlow, dFlow));

        }


    }

    static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        //<18333, bean1><18333, bean2>
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long sum_upFlow = 0;
            long sum_dFlow = 0;

            //计算每一个手机号总的上行流量和下行流量
            for (FlowBean bean : values) {
                sum_upFlow += bean.getUpFlow();
                sum_dFlow += bean.getdFlow();
            }

            FlowBean resultBean = new FlowBean(sum_upFlow, sum_dFlow);
            context.write(key, resultBean);

        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name","yarn");
        conf.set("yarn.resourcemanager.hostname","minimaster1");
        //conf.set("yarn.nodemanager.address","192.168.2.150");
        //conf.set( "mapred.job.tracker","192.168.2.150:9001");
       // conf.set("fs.default.name","hdfs:192.168.2.150:9000");
        Job job = Job.getInstance(conf);

        //指定本程序jar包所在的本地路径
//        job.setJar("/opt/jar/wordcount/wc.jar");
        job.setJarByClass(FlowCount.class);

        //指定本业务job要使用的mapper、Reducer业务类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //指定Map的输出数据K,V类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指定最总输出数据KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job输出的文件目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        job.submit();
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }
}
