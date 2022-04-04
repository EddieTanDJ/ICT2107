package sentiment;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class SentimentAnalysis {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SentimentAnalysis");
        job.setJarByClass(SentimentAnalysis.class);
        Path inPath = new Path("hdfs://localhost:9000/user/phamvanvung/project/input/");
        Path outPath = new Path("hdfs://localhost:9000/user/phamvanvung/project/output/");
        outPath.getFileSystem(conf).delete(outPath, true);
        job.addCacheFile(new URI("hdfs://localhost:9000/user/phamvanvung/project/AFINN-en-165.txt"));
        Configuration validationConf = new Configuration(false);
        ChainMapper.addMapper(job, SentimentValidationMapper.class, LongWritable.class, Text.class,
        		LongWritable.class, Text.class, validationConf);

        Configuration ansConf = new Configuration(false);
        ChainMapper.addMapper(job, SentimentMapper.class, LongWritable.class, Text.class,
                Text.class, IntWritable.class, ansConf);
        job.setMapperClass(ChainMapper.class);
        // job.setCombinerClass(SentimentReducer.class);
        job.setReducerClass(SentimentReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
