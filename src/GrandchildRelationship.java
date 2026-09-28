import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** 第三部分任务三：根据 child-parent 关系推导 grandchild-grandparent 关系。 */
public class GrandchildRelationship {

    public static class RelationshipMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] fields = line.split("\\s+");
            if (fields.length != 2) {
                throw new IOException("每行必须包含 child 和 parent 两列：" + line);
            }
            if ("child".equalsIgnoreCase(fields[0])
                    && "parent".equalsIgnoreCase(fields[1])) {
                return;
            }

            String child = fields[0];
            String parent = fields[1];

            // parent 的直接孩子是 child。
            outputKey.set(parent);
            outputValue.set("C:" + child);
            context.write(outputKey, outputValue);

            // child 的直接父母是 parent。
            outputKey.set(child);
            outputValue.set("P:" + parent);
            context.write(outputKey, outputValue);
        }
    }

    public static class RelationshipReducer
            extends Reducer<Text, Text, Text, Text> {
        private final Text grandchildOutput = new Text();
        private final Text grandparentOutput = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            grandchildOutput.set("grandchild");
            grandparentOutput.set("grandparent");
            context.write(grandchildOutput, grandparentOutput);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> children = new TreeSet<String>();
            Set<String> parents = new TreeSet<String>();

            for (Text value : values) {
                String tagged = value.toString();
                if (tagged.startsWith("C:")) {
                    children.add(tagged.substring(2));
                } else if (tagged.startsWith("P:")) {
                    parents.add(tagged.substring(2));
                }
            }

            for (String child : children) {
                for (String parent : parents) {
                    grandchildOutput.set(child);
                    grandparentOutput.set(parent);
                    context.write(grandchildOutput, grandparentOutput);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("用法：GrandchildRelationship <输入目录> <输出目录>");
            System.exit(1);
        }

        Job job = Job.getInstance(new Configuration(), "grandchild relationship");
        job.setJarByClass(GrandchildRelationship.class);
        job.setMapperClass(RelationshipMapper.class);
        job.setReducerClass(RelationshipReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

