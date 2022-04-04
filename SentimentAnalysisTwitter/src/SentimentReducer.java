package sentiment;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SentimentReducer extends Reducer<Text, IntWritable, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,  Reducer<Text, IntWritable, Text, Text>.Context context)
    		throws IOException,InterruptedException {
        String sentiment = "";
        for (IntWritable val : values) {
            if (val.get() == 0) {
                sentiment = "neutral";
            }
            else if (val.get() > 1) {
            	sentiment = "positive";
            }
            else {
            	sentiment = "negative";
            }
        }
        System.out.println(key + "\t" + sentiment);
        context.write(key , new Text(sentiment));
    }
}
