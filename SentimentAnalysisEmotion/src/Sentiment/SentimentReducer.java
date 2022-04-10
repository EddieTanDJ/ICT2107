package sentiment;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SentimentReducer extends Reducer<Text, Text, Text, Text> {
    Hashtable<String, Integer> emotion = new Hashtable<String, Integer>();

    private int angerCount = 0;
    private int anticipationCount = 0;
    private int disgustCount = 0;
    private int fearCount = 0;
    private int joyCount = 0;
    private int negativeCount = 0;
    private int positiveCount = 0;
    private int sadnessCount = 0;
    private int trustCount = 0;

	@Override
	public void reduce(Text key, Iterable<Text> values,  Reducer<Text, Text, Text, Text>.Context context)
    		throws IOException,InterruptedException {

		emotion.put("anger", 0);
		emotion.put("anticipation", 0);
		emotion.put("disgust", 0);
		emotion.put("fear", 0);
		emotion.put("joy", 0);
		emotion.put("negative", 0);
		emotion.put("positive", 0);
		emotion.put("sadness", 0);
		emotion.put("trust", 0);
	
        for (Text val : values) {
            String value = val.toString().trim().toLowerCase();
            if (value.equals("anger")) {
                angerCount++;
                emotion.put(value, emotion.get(value) + angerCount);
                //System.out.println("angerCount: " + angerCount);
            }
            else if (value.equals("anticipation")) {
                anticipationCount++;
                emotion.put(value, emotion.get(value) + anticipationCount);
                //System.out.println("anticipationCount: " + anticipationCount);
            }
            else if (value.equals("disgust")) {
                disgustCount++;
                emotion.put(value, emotion.get(value) + disgustCount);
               // System.out.println("disgustCount: " + disgustCount);
            }
            else if (value.equals("fear")) {
                fearCount++;
                emotion.put(value, emotion.get(value) + fearCount);
                // System.out.println("fearCount: " + fearCount);
            }
            else if (value.equals("joy")) {
                joyCount++;
                emotion.put(value, emotion.get(value) + joyCount);
                // System.out.println("joyCount: " + joyCount);
            }
            else if (value.equals("negative")) {
                negativeCount++;
                emotion.put(value, emotion.get(value) + negativeCount);
                // System.out.println("negativeCount: " + negativeCount);
            }
            else if (value.equals("positive")) {
                positiveCount++;
                emotion.put(value, emotion.get(value) + positiveCount);
                // System.out.println("positiveCount: " + positiveCount);
            }
            else if (value.equals("sadness")) {
                sadnessCount++;
                emotion.put(value, emotion.get(value) + sadnessCount);
                // System.out.println("sadnessCount: " + sadnessCount);
            }
            else if (value.equals("trust")) {
                trustCount++;
                emotion.put(value, emotion.get(value) + trustCount);
                // System.out.println("trustCount: " + trustCount);
            }
            else {
                // System.out.println("Error: " + value);
            }
            int max = 0;
            String maxKey = "";
            for (String data : emotion.keySet()) {
                if (emotion.get(data) > max) {
                	System.out.println(data);
                    max = emotion.get(data);
                    maxKey = data;
                }
            }
            // Write the result to the output file
            System.out.println(key + " " + maxKey);
     		context.write(key,  new Text(maxKey));
     		// Reset counter
     	    angerCount = 0;
     	    anticipationCount = 0;
     	    disgustCount = 0;
     	    fearCount = 0;
     	    joyCount = 0;
     	    negativeCount = 0;
     	    positiveCount = 0;
     	    sadnessCount = 0;
     	    trustCount = 0;
     	    // Reset overall variable
     	    max = 0;
     	    maxKey = "";
        }
    }

 	@Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
    		throws IOException, InterruptedException {
        // Find the maximum value of the Hashtable
        int max = 0;
        String maxKey = "";
        for (String key : emotion.keySet()) {
            if (emotion.get(key) > max) {
                max = emotion.get(key);
                maxKey = key;
            }
        }
        // Write the result to the output file
 		context.write(new Text("Overall Sentiment for all tweets:"),  new Text(maxKey));
    }
}
