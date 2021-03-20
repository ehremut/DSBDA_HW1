package bdtc.lab1;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class HW1Reducer extends Reducer<Text, IntWritable, Text, Text> {
    private static final String[] values_key =
            {"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"}; // список названий значений

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String keyDict = ""; // ключ
        Map<String, Integer> dict = new HashMap<String, Integer>(); // словарь для названия предупреждения и его количества
        while (values.iterator().hasNext()) { // итерация по элементам
            int element = values.iterator().next().get(); // записываем элемент
            int valueDict = 0; //инициализтируем переменную, которая будет служить значением для словаря
            keyDict = values_key[element]; // достаем ключ по индексу из массива предупреждений
            if (dict.get(keyDict) != null) { // проверка, что такое значение уже есть в словаре
                valueDict = dict.get(keyDict) + 1; // увеличиваем значение по этому ключу
                dict.put(keyDict, valueDict); // записываем в словарь
            }
            else {
                dict.put(keyDict, 1); // если встретилось впервые, то ставим значение 1
            }
        }

        String dictString = ""; // инициализируем результирующую строку
        Enumeration<String> strEnum = Collections.enumeration(dict.keySet()); // делаем итератор для хождения по ключам словаря
        while(strEnum.hasMoreElements()) {
            String localKey = strEnum.nextElement(); // сохраняем ключ локально
            dictString = dictString + " " + localKey + " : " + dict.get(localKey) + "; "; // пишем в результирующую строку
        }
        Text value = new Text(dictString); // переводим в Text()
        context.write(key, value); // записываем в context
    }
}
