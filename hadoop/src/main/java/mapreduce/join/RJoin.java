package mapreduce.join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * Created by Administrator on 2017/8/5.
 */
public class RJoin {

    static class RJoinMapper extends Mapper<LongWritable, Text, Text, InfoBean> {
        InfoBean bean = new InfoBean();
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            super.map(key, value, context);
        String line = value.toString();
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String name = inputSplit.getPath().getName();
        String pid = "";
            if (name.startsWith("order")) {
//                String[] field = fields;
            String[] fields = line.split(",");
            // id date pid amount
            pid = fields[2];
            bean.set(Integer.parseInt(fields[0]), fields[1], fields[2], Integer.parseInt(fields[3]), "", 0, 0, "0");

        } else {
            String[] fields = line.split(",");
            pid = fields[0];
            bean.set(0, "", fields[0], 0, fields[1], Integer.parseInt(fields[2]), Float.parseFloat(fields[3]), "1");
        }

            k.set(pid);
            context.write(k, bean);

    }
    }

    static class RJoinReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);
            InfoBean pdBean = new InfoBean(); //商品信息是唯一的
            ArrayList<InfoBean> orderBeans = new ArrayList<InfoBean>();
            for (InfoBean bean : values){
                if("1".equals(bean.getFlag()))//商品
                {
                    try {
                        BeanUtils.copyProperties(pdBean,bean);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }else
                {
                    InfoBean odbBean = new InfoBean();
                    try {
                        BeanUtils.copyProperties(odbBean,bean);
                        orderBeans.add(odbBean);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }

                }
            }

            //拼接两类数据形成最终结果
            for (InfoBean bean : orderBeans){
                bean.setPname(pdBean.getPname());
                bean.setCatehory_id(pdBean.getCatehory_id());
                bean.setPrice(pdBean.getPrice());

                context.write(bean,NullWritable.get());
            }


        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.set("mapred.textoutputformat.separator", "\t");

        Job job = Job.getInstance(conf);

        // 指定本程序的jar包所在的本地路径
        // job.setJarByClass(RJoin.class);
//		job.setJar("c:/join.jar");

        job.setJarByClass(RJoin.class);
        // 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(RJoinMapper.class);
        job.setReducerClass(RJoinReducer.class);

        // 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        // 指定最终输出的数据的kv类型
        job.setOutputKeyClass(InfoBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		/* job.submit(); */
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }

}
