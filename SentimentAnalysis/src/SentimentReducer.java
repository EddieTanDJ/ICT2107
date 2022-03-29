package sentiment;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SentimentReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Text value,  Reducer<Text, Text, Text, Text>.Context context)
    		throws IOException,InterruptedException {
		context.write(key,value);
    }
}