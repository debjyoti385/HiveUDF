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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.BooleanWritable;

public class HiveUDFSample extends GenericUDF {

    private  ListObjectInspector listOI;
    private  StringObjectInspector elementOI;
    private  ObjectInspector listElementOI;

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
//        System.out.println(a.getClass() + " " + b.getClass());
        if (!(a instanceof ListObjectInspector) || !(b instanceof StringObjectInspector)) {
            throw new UDFArgumentException("first argument must be a list / array, second argument must be a string");
        }
        this.listOI = (ListObjectInspector) a;
        this.elementOI = (StringObjectInspector) b;
        listElementOI = listOI.getListElementObjectInspector();

        // 2. Check that the list contains strings
        if(!(listOI.getListElementObjectInspector() instanceof StringObjectInspector)) {
            throw new UDFArgumentException("first argument must be a list of strings");
        }

        // the return type of our function is a boolean, so we provide the correct object inspector
        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        Object list = arguments[0].get();
        Object element = arguments[1].get();

        if (element == null || listOI.getListLength(list)<=0){
            return null;
        }


        for (int i=0; i<listOI.getListLength(list); ++i) {
            Object listElement = listOI.getListElement(list, i);
            if (listElement != null) {
                if (ObjectInspectorUtils.compare(element, elementOI,
                        listElement, listElementOI) == 0) {
                    return new BooleanWritable(true);
                }
            }
        }

        return new BooleanWritable(false);

    }
}
