package in.debjyotipaul.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

@Description(name = "difference of dates except Sunday", value = "_BUSINESS_DATE_DIFF_(hash_key, order_by_col1, order_by_col2 ...) "
		+ "- Returns the summed value of group", extended = "Example:\n"
		+ "  > SELECT _BUSINESS_DATE_DIFF_((to_date,from_date), order_by_col1, order_by_col2, ... ) FROM TABLE\n")
public class BusinessDayDiffUDF extends UDF {

    public BusinessDayDiffUDF() {
	}

    public LongWritable evaluate(Text inputStringDt1, Text inputStringDt2) throws HiveException {
        if (inputStringDt1 == null || inputStringDt2 == null) {
            return null;
        }
        return getDateDiff(inputStringDt1.toString(), inputStringDt2.toString());
    }

    public LongWritable getDateDiff(String endDateStr, String startDateStr) {
    	DateTime dateTimeStart,dateTimeEnd;
		//DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
		DateTimeParser[] parsers = { 
		        DateTimeFormat.forPattern( "yyyy-MM-dd HH:mm:ss" ).getParser(),
		        DateTimeFormat.forPattern( "yyyy-MM-dd" ).getParser() };
		DateTimeFormatter formatter = new DateTimeFormatterBuilder().append( null, parsers ).toFormatter();
		
        if (startDateStr.contains("T") || startDateStr.contains("t")){
            dateTimeStart = new DateTime(javax.xml.bind.DatatypeConverter.parseDateTime(startDateStr).getTime());
        }
		else {
			dateTimeStart = formatter.parseDateTime(startDateStr);
		}

        if (endDateStr.contains("T") || endDateStr.contains("t")){
            dateTimeEnd = new DateTime(javax.xml.bind.DatatypeConverter.parseDateTime(endDateStr).getTime());
        }
		else {
			dateTimeEnd = formatter.parseDateTime(endDateStr);
		}
        int daysBtwDates = Days.daysBetween(new LocalDate(dateTimeStart),new LocalDate(dateTimeEnd)).getDays();
        int weeksBetween = dateTimeEnd.getWeekOfWeekyear() - dateTimeStart.getWeekOfWeekyear();
        if(daysBtwDates>0){
        	if(dateTimeStart.getDayOfWeek() == DateTimeConstants.SUNDAY && dateTimeEnd.getDayOfWeek() != DateTimeConstants.SUNDAY) {
        		weeksBetween --;
        	}
        	return new LongWritable(daysBtwDates - weeksBetween);
        }
        else {
        	if(dateTimeEnd.getDayOfWeek() == DateTimeConstants.SUNDAY && dateTimeStart.getDayOfWeek() != DateTimeConstants.SUNDAY) {
        		weeksBetween ++;
        	}
        	return new LongWritable(daysBtwDates + weeksBetween);
        }

    }
}
