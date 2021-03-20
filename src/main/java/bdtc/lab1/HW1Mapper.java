package bdtc.lab1;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.*;

// mapper class description
public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(); // variable value initialization
    private Text word = new Text(); // key initialization
    private static String regexStr = "^20[0-9]{2}-(0[1-9]|1[0-2])-(0[1-9]|1[0-9]|2[0-9]|3[0-1]) (0[0-9]|1[0-9]|2[0-3])(:(0[0-9]|[1-5][0-9])){2},[0-7]$";
    Pattern pattern = Pattern.compile(regexStr);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Matcher matcher = pattern.matcher(line);
        if (!matcher.find()) {
            // increment counter if error
            context.getCounter(CounterType.MALFORMED).increment(1);

        } else {
            // get elements of good line with split
            String[] stringElements = line.split("[:,]");
            // check for empty
            if (Arrays.stream(stringElements).findFirst().isPresent() & !stringElements[stringElements.length - 1].isEmpty()) {
                // set key
                word.set(Arrays.stream(stringElements).findFirst().orElse(""));
                // set value
                one.set(Integer.parseInt(stringElements[stringElements.length - 1]));
                // write to context
                context.write(word, one);
            }
        }
    }
}
