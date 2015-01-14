package in.debjyotipaul.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.Text;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateISOToSqlUDF extends UDF {
    private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Calendar calendar = Calendar.getInstance();
    private Timestamp result = new Timestamp(0);

    public DateISOToSqlUDF() {
    }

    /**
     * Returns sqlTimeStamp from a given date in ISO8601 Date Format or below mentioned formats.
     *
     * @param dateText
     *          the dateString in the format of "yyyy-MM-dd HH:mm:ss" or
     *          "yyyy-MM-dd HH:mm:ss.SSS" or "Date ISO 8601 Format".
     * @return  Sql TimeStamp
     */
    public Text evaluate(Text dateText) {

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
            result.setTime(calendar.getTimeInMillis());
            return new Text(result.toString());
        } catch (ParseException e) {
            return new Text(result.toString());
        }
    }

    public Text evaluate(TimestampWritable t) {
        if (t == null) {
            return null;
        }
        result = t.getTimestamp();
        return new Text(result.toString());
    }
}
