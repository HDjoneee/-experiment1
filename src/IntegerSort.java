import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** 第三部分任务二：对多个输入文件中的整数进行全局升序排序并编号。 */
public class IntegerSort {

    public static class SortMapper
            extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
        private final IntWritable outputKey = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            try {
                outputKey.set(Integer.parseInt(line));
                context.write(outputKey, NullWritable.get());
            } catch (NumberFormatException exception) {
                throw new IOException("输入内容不是整数：" + line, exception);
            }
        }
    }

    public static class SortReducer
            extends Reducer<IntWritable, NullWritable, LongWritable, IntWritable> {
        private long rank = 1L;
        private final LongWritable outputRank = new LongWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<NullWritable> values,
                              Context context)
                throws IOException, InterruptedException {
            for (NullWritable ignored : values) {
                outputRank.set(rank++);
                context.write(outputRank, key);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("用法：IntegerSort <输入目录> <输出目录>");
            System.exit(1);
        }

        Job job = Job.getInstance(new Configuration(), "global integer sort");
        job.setJarByClass(IntegerSort.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

