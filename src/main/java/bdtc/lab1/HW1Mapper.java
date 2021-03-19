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


public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable();
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (!Pattern.matches("^20[0-9]{2}\\-(0[1-9]|1[0-2])\\-(0[1-9]|1[0-9]|2[0-9]|3[0-1])\\ (0[0-9]|1[0-9]|2[0-3])(\\:(0[0-9]|[1-5][0-9])){2}\\,[0-7]$", line)) {
            context.getCounter(CounterType.MALFORMED).increment(1);
        } else {
            String[] stringElements = line.split("[:,]");
            if (Arrays.stream(stringElements).findFirst().isPresent() & !stringElements[stringElements.length - 1].isEmpty()) {
                word.set(Arrays.stream(stringElements).findFirst().orElse(""));
                one.set(Integer.parseInt(stringElements[stringElements.length - 1]));
                context.write(word, one);
            }
        }
    }
}
