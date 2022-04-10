package sentiment;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
     Hashtable<String, String> AFINN_map = new Hashtable<String, String>();

    public void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException {
        // get the list of files from DistributedCache
        BufferedReader br = new BufferedReader(new FileReader("AFINN-en-165.txt"));
        String line = null;
        // Extract out the AFINN word scores
        while ((line = br.readLine()) != null) {
            String splits[] = line.split("\t");
            AFINN_map.put(splits[0], splits[1]);
        }
        br.close();
    }

    @Override
    public void map(LongWritable key, Text value,  Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String parts[] = value.toString().split(";");
        // Get the comments
        String comments = parts[6];
        // Get each word in the comments
        String words[] = comments.split(" ");
        int sentiment_sum = 0;
        for (String word : words) {
        	// Remove Punctuation in words
        	word = word.replaceAll("[^A-Za-z0-9]","");
            if (AFINN_map.get(word) != null) {
                Integer x = new Integer(AFINN_map.get(word));
                sentiment_sum += x;
            }
        }
        // Emit the tweet and its sentiment score
        System.out.println(sentiment_sum);
        context.write(new Text(comments), new IntWritable(sentiment_sum));
    }
}