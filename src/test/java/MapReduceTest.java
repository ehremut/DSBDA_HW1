import eu.bitwalker.useragentutils.UserAgent;
import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class MapReduceTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, MapWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, MapWritable> mapReduceDriver;

    private final String testLine = "2021-03-05 05:05:05,1";
    private final String testLine1 = "2021-03-05 05:05:05,1";
    private final String testLine2 = "2021-03-05 05:08:05,4";
    private final String testKey = "2021-03-05 05";

    private UserAgent userAgent;
    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testLine))
                .withOutput(new Text("2021-03-05 05"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(5));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(2));
        MapWritable map = new MapWritable();
        map.put(new Text("alert"), new IntWritable(3));
        map.put(new Text("crit"), new IntWritable(1));
        map.put(new Text("notice"), new IntWritable(1));
        reduceDriver
                .withInput(new Text(testKey), values)
                .withOutput(new Text(testKey), map)
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        MapWritable result = new MapWritable();
        result.put(new Text("alert"), new IntWritable(1));
        result.put(new Text("warning"), new IntWritable(1));
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testLine1))
                .withInput(new LongWritable(), new Text(testLine2))
                .withOutput(new Text(testKey), result)
                .runTest();
    }
}
