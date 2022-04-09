package sentiment;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SentimentValidationMapper extends Mapper<LongWritable, Text, LongWritable, Text>  {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,
            LongWritable, Text>.Context context) throws IOException, InterruptedException {
        // If the line is valid, split the line by comma and write the key-value pair
        if(isValid(value.toString())) {
            context.write(key, value);
        }
    }

    /**
     * Checking the input line is valid or not
     * @param line
     * @return boolean
     */
    private boolean isValid(String line){
        String[] parts = line.split(",");
        if (parts.length == 8) {
            return true;
        } else {
            return false;
        }
    }

}
