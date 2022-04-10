package sentiment;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SentimentMapper extends Mapper<LongWritable, Text, Text, Text> {
    Hashtable<String, String> EMOTION_MAP = new Hashtable<String, String>();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        // We will put the stopwords-en.txt to Distributed Cache in the driver class
        // so we can access to it here locally its name
        BufferedReader br = new BufferedReader(new FileReader("Emotion.txt"));
        String line = null;
        while ((line = br.readLine()) != null) {
            String splits[] = line.split("\t");
            if (isEmotion(line)) {
                EMOTION_MAP.put(splits[0], splits[1]);
            }
        }
        br.close();
    }

    @Override
    public void map(LongWritable key, Text value,  Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String parts[] = value.toString().split(";");
        // Get the comments
        String comments = parts[6];
        // Get each word in the comments
        String words[] = comments.split(" ");
        for (String word : words) {
        	// Remove Punctuation in words
        	word = word.replaceAll("[^A-Za-z0-9]","");
            if (EMOTION_MAP.get(word) != null) {
                String x = EMOTION_MAP.get(word);
               // System.out.println(comments + "\t" + x);
                context.write(new Text(comments), new Text(x));
            }
        }
    }

    /**
     * Checking emotion is valid or not
     * @param line
     * @return boolean
     */
    private boolean isEmotion(String line) {
        String[] parts = line.split("\t");
        if (parts.length == 3) {
            if (parts[2].contains("1")) {
                return true;
            }
            return false;
        } else {
            return false;
        }
    }

}