package bdtc.lab1;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

// Reducer class definition
public class HW1Reducer extends Reducer<Text, IntWritable, Text, MapWritable> {
    private static final String[] values_key =
            {"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"}; // array of keys

    // reduce method definition
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // create writeable key
        Text keyWritable = new Text();
        // create writable value
        MapWritable dictWritable = new MapWritable();
        // iterating over elements
        while (values.iterator().hasNext()) {
            // get element
            int element = values.iterator().next().get();
            // create local value for dict
            IntWritable valueWritable = new IntWritable();
            // set local value for key in writeable key
            keyWritable.set(new Text(values_key[element]));
            // check if key not first
            if (dictWritable.get(keyWritable) != null) {
                // get exist value
                valueWritable = (IntWritable) dictWritable.get(keyWritable);
                // increment count of exist value
                valueWritable.set(valueWritable.get() + 1);
                // update dict with new value coint
                dictWritable.put(new Text(keyWritable), valueWritable);
            }
            else {
                // if first key then set 1
                valueWritable.set(1);
                // update dict with new value coint
                dictWritable.put(new Text(keyWritable), valueWritable);
            }
        }
        // write to context
        context.write(key, dictWritable);
    }
}
