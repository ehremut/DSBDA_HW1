package bdtc.lab1;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class HW1Reducer extends Reducer<Text, IntWritable, Text, Text> {
    private static final String[] values_key =
            {"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"}; // array of keys

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String keyDict = "";
        Map<String, Integer> dict = new HashMap<String, Integer>();
        while (values.iterator().hasNext()) {
            int element = values.iterator().next().get();
            int valueDict = 0;
            keyDict = values_key[element];
            if (dict.get(keyDict) != null) {
                valueDict = dict.get(keyDict) + 1;
                dict.put(keyDict, valueDict);
            }
            else {
                dict.put(keyDict, 1);
            }
        }

        String dictString = "";
        Enumeration<String> strEnum = Collections.enumeration(dict.keySet());
        while(strEnum.hasMoreElements()) {
            String localKey = strEnum.nextElement();
            dictString = dictString + " " + localKey + " : " + dict.get(localKey) + "; ";
        }
        Text value = new Text(dictString);
        context.write(key, value);
    }
}
