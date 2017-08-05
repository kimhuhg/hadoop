package mapreduce.mapsidejoin;

import mapreduce.join.InfoBean;
import mapreduce.join.RJoin;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017/8/5.
 */
public class MapSideJoin {

    static class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        Map<String, String> pdInfoMap = new HashMap<String, String>();
        Text text = new Text();

        /**
         * 通过阅读父类Mapper的源码，发现 setup方法是在maptask处理数据之前调用一次 可以用来做一些初始化工作
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("H:\\project\\hadoop\\hadoop\\data\\product.txt")));
            String line;
            while (StringUtils.isNotEmpty(line = br.readLine())) {
                String[] fields = line.split(",");
                pdInfoMap.put(fields[0], fields[1]);
            }
        }

        /**
         * 由于已经持有完整的产品信息表，所以在map方法中就能实现join逻辑了
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String orderLine = value.toString();
            String[] fields = orderLine.split(",");
            String pdName = pdInfoMap.get(fields[1]);
            text.set(orderLine + "\t" + pdName);
            context.write(text,NullWritable.get());

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        // 指定本程序的jar包所在的本地路径
        // job.setJarByClass(RJoin.class);
//		job.setJar("c:/join.jar");

        job.setJarByClass(MapSideJoin.class);
        // 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(MapSideJoinMapper.class);

        // 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 指定job的输入原始文件所在目录   order.txt  => args[0]
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /* job.submit(); */

        // 指定需要缓存一个文件到所有的maptask运行节点工作目录
        /* job.addArchiveToClassPath(archive); */// 缓存jar包到task运行节点的classpath中
		/* job.addFileToClassPath(file); */// 缓存普通文件到task运行节点的classpath中
		/* job.addCacheArchive(uri); */// 缓存压缩包文件到task运行节点的工作目录
		/* job.addCacheFile(uri) */// 缓存普通文件到task运行节点的工作目录

        // 将产品表文件缓存到task工作节点的工作目录中去
        job.addCacheFile(new URI("H:\\project\\hadoop\\hadoop\\data\\product.txt"));

        job.setNumReduceTasks(0);
//		job.addArchiveToClassPath();
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

}
