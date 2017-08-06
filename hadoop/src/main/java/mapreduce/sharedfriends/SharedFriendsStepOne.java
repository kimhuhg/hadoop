package mapreduce.sharedfriends;

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
 * A:B,C,D,F,E,O
 * 方法一：
 * Created by Administrator on 2017/8/6.
 */
public class SharedFriendsStepOne {
    static class SharedFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] person_friends = value.toString().split(":");
            String person = person_friends[0];
            String friends = person_friends[1];
            for (String friend : friends.split(",")) {
                context.write(new Text(friend), new Text(person));
            }

        }
    }

    static class SharedFriendsStepOneReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuffer sb = new StringBuffer();

            for (Text person : values) {
                sb.append(person).append(",");

            }
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(SharedFriendsStepOne.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SharedFriendsStepOneMapper.class);
        job.setReducerClass(SharedFriendsStepOneReducer.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }

}

/**
 A	I,K,C,B,G,F,H,O,D,
 B	A,F,J,E,
 C	A,E,B,H,F,G,K,
 D	G,C,K,A,L,F,E,H,
 E	G,M,L,H,A,F,B,D,
 F	L,M,D,C,G,A,
 G	M,
 H	O,
 I	O,C,
 J	O,
 K	B,
 L	D,E,
 M	E,F,
 O	A,H,I,J,F,

 */
