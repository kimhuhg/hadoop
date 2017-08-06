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
import java.util.Arrays;

/**
 * Created by Administrator on 2017/8/6.
 */
public class SharedFriendsStepTwo {
    static class SharedFriendsStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] friend_persons = value.toString().split("\t");
            String friend = friend_persons[0];
            String[] persons = friend_persons[1].split(",");

            Arrays.sort(persons);

            for (int i = 0; i < persons.length - 1; i++) {
                for (int j = i + 1; j < persons.length; j++) {
                    // 发出 <人-人，好友> ，这样，相同的“人-人”对的所有好友就会到同1个reduce中去
                    context.write(new Text(persons[i] + "-" + persons[j]), new Text(friend));
                }

            }

        }
    }

    static class SharedFriendsStepTwoReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text person_person, Iterable<Text> friends, Context context) throws IOException, InterruptedException {

            StringBuffer sb = new StringBuffer();

            for (Text friend : friends) {
                sb.append(friend).append(" ");

            }
            context.write(person_person, new Text(sb.toString()));
        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(SharedFriendsStepTwo.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SharedFriendsStepTwoMapper.class);
        job.setReducerClass(SharedFriendsStepTwoReducer.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}

/**
 A-B	E C
 A-C	D F
 A-D	E F
 A-E	D B C
 A-F	O B C D E
 A-G	F E C D
 A-H	E C D O
 A-I	O
 A-J	O B
 A-K	D C
 A-L	F E D
 A-M	E F
 B-C	A
 B-D	A E
 B-E	C
 B-F	E A C
 B-G	C E A
 B-H	A E C
 B-I	A
 B-K	C A
 B-L	E
 B-M	E
 B-O	A
 C-D	A F
 C-E	D
 C-F	D A
 C-G	D F A
 C-H	D A
 C-I	A
 C-K	A D
 C-L	D F
 C-M	F
 C-O	I A
 D-E	L
 D-F	A E
 D-G	E A F
 D-H	A E
 D-I	A
 D-K	A
 D-L	E F
 D-M	F E
 D-O	A
 E-F	D M C B
 E-G	C D
 E-H	C D
 E-J	B
 E-K	C D
 E-L	D
 F-G	D C A E
 F-H	A D O E C
 F-I	O A
 F-J	B O
 F-K	D C A
 F-L	E D
 F-M	E
 F-O	A
 G-H	D C E A
 G-I	A
 G-K	D A C
 G-L	D F E
 G-M	E F
 G-O	A
 H-I	O A
 H-J	O
 H-K	A C D
 H-L	D E
 H-M	E
 H-O	A
 I-J	O
 I-K	A
 I-O	A
 K-L	D
 K-O	A
 L-M	E F
 */
