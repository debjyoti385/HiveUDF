package in.debjyotipaul.udf;

/**
 * Created with IntelliJ IDEA.
 * User: debjyoti.paul
 * Date: 12/31/13
 * Time: 3:58 PM
 * To change this template use File | Settings | File Templates.
 */


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class HiveUDFSampleEqual extends GenericUDF {

    private StringObjectInspector elementOI1;
    private StringObjectInspector elementOI2;
    private transient ObjectInspectorConverters.Converter[] converters;

    @Override
    public String getDisplayString(String[] args) {
        return "arrayContainsExample()"; // this should probably be better
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("arrayContainsExample only takes 2 arguments: List<T>, T");
        }
        // 1. Check we received the right object types.
        ObjectInspector a = arguments[0];
        ObjectInspector b = arguments[1];
        if (!(a instanceof StringObjectInspector) || !(b instanceof StringObjectInspector)) {
            throw new UDFArgumentException("first argument must be a list / array, second argument must be a string");
        }
        this.elementOI1 = (StringObjectInspector) a;
        this.elementOI2 = (StringObjectInspector) b;



        converters = new ObjectInspectorConverters.Converter[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }

        System.out.println(" inside Initialized ");

        // the return type of our function is a boolean, so we provide the correct object inspector
        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        // get the list and string from the deferred objects using the object inspectors
        // check for nulls
        if (arguments[0].get() == null || arguments[1].get() == null) {
            return null;
        }

        Text str1 = (Text) converters[0].convert(arguments[0].get());
        Text str2 = (Text) converters[1].convert(arguments[1].get());

        // see if our list contains the value we need
        if (str1.equals(str2)){
            return new BooleanWritable(true);
        }

        System.out.println("inside EVALUATE");
        return new BooleanWritable(false);
    }




}
