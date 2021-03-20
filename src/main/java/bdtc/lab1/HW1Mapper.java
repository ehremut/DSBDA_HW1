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
    private static String regexStr = "^20[0-9]{2}-(0[1-9]|1[0-2])-(0[1-9]|1[0-9]|2[0-9]|3[0-1]) (0[0-9]|1[0-9]|2[0-3])(:(0[0-9]|[1-5][0-9])){2},[0-7]$"; //регулярное выражение
    Pattern pattern = Pattern.compile(regexStr); // компилляция регулярного выражения

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();  // перевод строки в string
        Matcher matcher = pattern.matcher(line); //поиск совпадений
        if (!matcher.find()) { // условие если не найдено
            context.getCounter(CounterType.MALFORMED).increment(1); // инкрементим счетчик при ошибке

        } else {
            String[] stringElements = line.split("[:,]"); //разбиваем строку на ключ и значение
            if (Arrays.stream(stringElements).findFirst().isPresent() & !stringElements[stringElements.length - 1].isEmpty()) { // проверка, что полученные ключ и значение не пустые
                word.set(Arrays.stream(stringElements).findFirst().orElse("")); // сеттим ключ
                one.set(Integer.parseInt(stringElements[stringElements.length - 1])); // сеттим значение
                context.write(word, one); // записываем в контекст
            }
        }
    }
}
