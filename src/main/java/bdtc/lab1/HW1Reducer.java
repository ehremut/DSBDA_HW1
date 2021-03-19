package bdtc.lab1;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class HW1Reducer extends Reducer<Text, IntWritable, Text, MapWritable> {
    private static final String[] values_key =
            {"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"};


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Text keyWritable = new Text();
        MapWritable dictWritable = new MapWritable();
        while (values.iterator().hasNext()) {
            int element = values.iterator().next().get();
            IntWritable valueWritable = new IntWritable();
            keyWritable.set(new Text(values_key[element]));
            if (dictWritable.get(keyWritable) != null) {
                valueWritable = (IntWritable) dictWritable.get(keyWritable);
                valueWritable.set(valueWritable.get() + 1);
                dictWritable.put(new Text(keyWritable), valueWritable);
            }
            else {
                valueWritable.set(1);
                dictWritable.put(new Text(keyWritable), valueWritable);
            }
        }
        context.write(key, dictWritable);
    }
}
