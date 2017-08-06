package mapreduce.InverIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 2017/8/6.
 * (hello->a.txt,3),(tom->a.txt,2)
 */
public class InverIndexStepTwo {
    static class InverIndexStepTwoMapper extends Mapper<LongWritable,Text,Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("-->");
            context.write(new Text(fields[0]),new Text(fields[1]));
        }
    }

    static class InverIndexStepTwoReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuffer sb = new StringBuffer();
            for(Text value : values){
                sb.append(value.toString().replace("\t","-->") + "\t");

            }
            context.write(key,new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 1 || args == null) {
            args = new String[]{"D:/temp/out/part-r-00000", "D:/temp/out2"};
        }

        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setJarByClass(InverIndexStepTwo.class);

        job.setMapperClass(InverIndexStepTwoMapper.class);
        job.setReducerClass(InverIndexStepTwoReducer.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 1:0);
    }

}
