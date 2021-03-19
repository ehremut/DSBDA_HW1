package bdtc.lab1;
import java.util.*;
import java.text.*;
import java.io.*;
import java.text.ParseException;

//public class DateStruct {
//
//}

public class GenerateInputData {

    public static void main(String[] args) {

        String start = "01/01/2012 01:01:01";
        String end = "01/03/2012 01:01:01";
        DateFormat format = new SimpleDateFormat("dd/mm/yyyy HH:mm:ss", Locale.ENGLISH);
        try {
            Date date = format.parse(start);
            date.getTime();
            System.out.println("debug");
            System.out.println(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    Date getTime(Date date) {
        System.out.println(date);
        return date;
    }

//        void random_date(start, end) {
//            delta = end - start
//            int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
//            random_second = randrange(int_delta)
//            return start + timedelta(seconds = random_second)
//
//            warnings = ['debug', 'info', 'notice', 'warning', 'warn', 'emerg', 'panic', 'error', 'err', 'crit', 'alert']
//            start = input('input start date')
//            if re.match("(0[1-9]|1[0-2])/(0[1-9]|2[0-9]|3[0-1])/20[0-9]{2} (0[0-9]|1[0-9]|2[0-3])(:(0[0-9]|[1-5][0-9])){2}", start) == None:
//            print("BAD START DATE")
//            exit(1)
//            end = input('input end date')
//            if
//            re.match("(0[1-9]|1[0-2])/(0[1-9]|2[0-9]|3[0-1])/20[0-9]{2} (0[0-9]|1[0-9]|2[0-3])(:(0[0-9]|[1-5][0-9])){2}", end) == None:
//            print("BAD END DATE")
//            exit(1)
//
//            d1 = datetime.strptime(start, '%m/%d/%Y %H:%M:%S')
//            d2 = datetime.strptime(end, '%m/%d/%Y %H:%M:%S')
//            qty_files = d2.day - d1.day + 1
//            for i in range(qty_files):
//            for j in range(300000):
//            print(random_date(d1, d2), random.choice(warnings))
//        }
}
