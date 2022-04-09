package sentiment;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SentimentReducer extends Reducer<Text, IntWritable, Text, Text> {
	Hashtable<String, Integer> sentimentsscore = new Hashtable<String, Integer>();
	int positivecount = 0;
	int neturalcount = 0;
	int negativecount = 0;

	@Override
	public void reduce(Text key, Iterable<IntWritable> values,  Reducer<Text, IntWritable, Text, Text>.Context context)
    		throws IOException,InterruptedException {
		sentimentsscore.put("positive", 0);
		sentimentsscore.put("netural", 0);
		sentimentsscore.put("negative", 0);
        String sentiment = "";
        for (IntWritable val : values) {
            if (val.get() == 0) {
                sentiment = "neutral";
                neturalcount++;
                sentimentsscore.put("netural", sentimentsscore.get("netural") + neturalcount);
            }
            else if (val.get() > 1) {
            	sentiment = "positive";
            	positivecount++;
            	sentimentsscore.put("positive", sentimentsscore.get("positive") + positivecount);
            }
            else {
            	sentiment = "negative";
            	negativecount++;
            	sentimentsscore.put("negative", sentimentsscore.get("negative") + negativecount);
            }
        }
        System.out.println(key + "\t" + sentiment);
        context.write(key , new Text(sentiment));
    }
		
 	@Override
    protected void cleanup(Reducer<Text, IntWritable, Text, Text>.Context context)
    		throws IOException, InterruptedException {
        // Find the maximum value of the Hashtable
        int max = 0;
        String maxKey = "";
        for (String key : sentimentsscore.keySet()) {
            if (sentimentsscore.get(key) > max) {
                max = sentimentsscore.get(key);
                maxKey = key;
            }
        }
        System.out.println("Overall Sentiment for all tweets:" + maxKey);
        // Write the result to the output file
 		context.write(new Text("Overall Sentiment for all tweets:"),  new Text(maxKey));
    }
}


