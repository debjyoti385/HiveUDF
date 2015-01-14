package in.debjyotipaul.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created with IntelliJ IDEA.
 * User: debjyoti.paul
 * Date: 1/2/14
 * Time: 12:46 PM
 * To change this template use File | Settings | File Templates.
 */

@Description(
        name="SimpleUDFExample",
        value="returns 'hello x', where x is whatever you give it (STRING)",
        extended="SELECT simpleudfexample('world') from foo limit 1;"
)

public class HiveUDFSimpleSample extends UDF {

    public Text evaluate(Text input) {
        if(input == null) return null;
        return new Text("Hello " + input.toString());
    }

}
