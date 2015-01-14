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


@Description(name = "datelookup",
    value = "_FUNC_(date) - Returns the date_key for date_dimension",
    extended = "date is a string in the format of 'yyyy-MM-dd HH:mm:ss' or "
    + "'yyyy-MM-dd'.\n"
    + "Example:\n "
    + "  > SELECT _FUNC_('2012-01-15') FROM src LIMIT 1;\n" + "  20120115")
public class DateLookupUDF extends UDF {
  private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private Calendar calendar = Calendar.getInstance();

  private IntWritable result = new IntWritable();

  public DateLookupUDF() {
  }

  /**
   * Get the year from a date string.
   *
   * @param dateText
   *          the dateString in the format of "yyyy-MM-dd HH:mm:ss" or
   *          "yyyy-MM-dd".
   * @return an int with yyyy*(10000)+MM*(100)+dd and 0 if the dateString is not a valid date
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
      }
      else{
          date = formatter.parse(dateString);
          calendar.setTime(date);
      }
      int temp = 1;
      temp = calendar.get(Calendar.YEAR) * 100;
      temp = (temp + calendar.get(Calendar.MONTH) + 1) * 100;
      temp = temp + calendar.get(Calendar.DAY_OF_MONTH);
      result.set(temp);
      return result;
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
      temp = calendar.get(Calendar.YEAR) * 100;
      temp = (temp + calendar.get(Calendar.MONTH) + 1) * 100;
      temp = temp + calendar.get(Calendar.DAY_OF_MONTH);
      result.set(temp);
//    result.set(calendar.get(Calendar.YEAR));
    return result;
  }

}
