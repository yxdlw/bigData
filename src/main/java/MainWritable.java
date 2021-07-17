import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author chenxuebing
 * @discraption
 * @date 2021/7/18 12:05 上午
 */
public class MainWritable {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/chenxuebing/bigData/HTTP_20130313143750.dat","/Users/chenxuebing/demo/bigData"};

        //1.获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.加载jar包
        job.setJarByClass(MainWritable.class);

        //3.关联map和reduce
        job.setMapperClass(MapWritable.class);
        job.setReducerClass(ReduceWritable.class);

        //4.设置最终输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //5.设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //6.提交job任务
        job.waitForCompletion(true);
    }
}

