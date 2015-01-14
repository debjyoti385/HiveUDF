package in.debjyotipaul.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

@Description(name = "timelookup",
    value = "_FUNC_(date) - Returns the date_key for date_dimension",
    extended = "date is a string in the format of 'yyyy-MM-dd HH:mm:ss' or "
    + "'yyyy-MM-dd'.\n"
    + "Example:\n "
    + "  > SELECT _FUNC_('2012-01-15 06:12:15') FROM src LIMIT 1;\n" + "  612")
public class TimeLookupUDF extends UDF {
  private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private Calendar calendar = Calendar.getInstance();

  private IntWritable result = new IntWritable();

  public TimeLookupUDF() {
  }

  /**
   * Get the year from a date string.
   *
   * @param dateText
   *          the dateString in the format of "yyyy-MM-dd HH:mm:ss" or
   *          "yyyy-MM-dd".
   * @return an int with HH*(100)+mm and 0 if the dateString is not a valid date
   *         string.
   */
  public IntWritable evaluate(Text dateText) {

    if (dateText == null) {
      return null;
    }
    Date date = new Date();
    try {
        String dateString = dateText.toString();
        if (dateString.contains("T") || dateString.contains("t")){
            calendar = javax.xml.bind.DatatypeConverter.parseDateTime(dateString);
            int temp = 1;
            temp = calendar.get(Calendar.HOUR_OF_DAY) * 100;
            temp = temp + calendar.get(Calendar.MINUTE);
            result.set(temp);
            return result;
        }
        else {
          date = formatter.parse(dateString);
          calendar.setTime(date);
          int temp = 1;
          temp = calendar.get(Calendar.HOUR_OF_DAY) * 100;
          temp = temp + calendar.get(Calendar.MINUTE);
          result.set(temp);
          return result;
      }
    } catch (ParseException e) {
        result.set(0);
        return result;
    }
  }

  public IntWritable evaluate(TimestampWritable t) {
    if (t == null) {
      return null;
    }

    calendar.setTime(t.getTimestamp());
      int temp =1 ;
      temp = calendar.get(Calendar.HOUR_OF_DAY) * 100;
      temp = temp + calendar.get(Calendar.MINUTE);
      result.set(temp);
    return result;
  }

}
