package mapreduce.flowcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountSort {

    /**
     * map 输出的数据要分区6各分区
     * 重写partitioner 分到各个区
     */
    static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
        FlowBean bean = new FlowBean();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //拿到的是上一个统计程序的输出结果，已经是各手机号的总流量信息
            //获取一行内容转化成数据
            String line = value.toString();
            //切分字段
            String[] fields = line.split("\t");
            //取出手机号
            String phoneNumber = fields[0];
            //取出上行下行流量
            long upFlow = Long.parseLong(fields[1]);
            long dFlow = Long.parseLong(fields[2]);

            bean.set(upFlow, dFlow);
            v.set(phoneNumber);
            context.write(bean, v);

        }


    }

    static class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
        //<18333, bean1><18333, bean2>
        @Override
        protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(values.iterator().next(), key);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "minimaster1");
        //conf.set("yarn.nodemanager.address","192.168.2.150");
        //conf.set( "mapred.job.tracker","192.168.2.150:9001");
        // conf.set("fs.default.name","hdfs:192.168.2.150:9000");
        Job job = Job.getInstance(conf);

        //指定本程序jar包所在的本地路径
//        job.setJar("/opt/jar/wordcount/wc.jar");
        job.setJarByClass(mapreduce.flowcount.FlowCountSort.class);

        //指定本业务job要使用的mapper、Reducer业务类
        job.setMapperClass(mapreduce.flowcount.FlowCountSort.FlowCountSortMapper.class);
        job.setReducerClass(mapreduce.flowcount.FlowCountSort.FlowCountSortReducer.class);

        //指定自定义的数据分区器
        // job.setPartitionerClass(ProvincePartitioner.class);
        //同时指定相应数量的reduceTask
        //job.setNumReduceTasks(5);

        //指定Map的输出数据K,V类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //指定最总输出数据KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path outpath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outpath)){
            fs.delete(outpath,true);
        }

        //指定job输出的文件目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        job.submit();
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }


}
