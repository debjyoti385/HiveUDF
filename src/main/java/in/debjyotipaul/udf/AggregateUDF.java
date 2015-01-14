package in.debjyotipaul.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;

import java.util.*;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.LIST;

/**
 * Created with IntelliJ IDEA.
 * User: debjyoti.paul
 * Date: 5/16/14
 * Time: 7:04 PM
 * To change this template use File | Settings | File Templates.
 */


    @Description(name = "array_struct_min_max_sum",
            value = "_FUNC_(array(struct1,struct2,...), string myfield) - "
                    + "returns the passed array struct, ordered by the given field  " ,
            extended = "Example:\n"
                    + "  > SELECT _FUNC_(str, 'myfield','SUM'/'MIN'/'MAX') FROM src LIMIT 1;\n"
                    + " 'b' ")
    public class AggregateUDF extends GenericUDF {
        protected ObjectInspector[] argumentOIs;

        ListObjectInspector loi;
        StructObjectInspector elOi;
        String operation;
        String fieldName;

        Map<String,StructFieldComparator> comparatorCache = new HashMap<String,StructFieldComparator>();

        @Override
        public ObjectInspector initialize(ObjectInspector[] ois) throws UDFArgumentException {

            argumentOIs = ois;
            comparatorCache.clear();

            return checkAndReadObjectInspectors(ois);
        }

        protected ObjectInspector checkAndReadObjectInspectors(ObjectInspector[] ois)
                throws UDFArgumentTypeException, UDFArgumentException {
            if(ois.length != 3 ) {
                throw new UDFArgumentException("2 arguments needed, found " + ois.length );
            }

            // first argument must be a list/array
            if (! ois[0].getCategory().equals(LIST)) {
                throw new UDFArgumentTypeException(0, "Argument 1"
                        + " of function " + this.getClass().getCanonicalName() + " must be " + Constants.LIST_TYPE_NAME
                        + ", but " + ois[0].getTypeName()
                        + " was found.");
            }

            // a list/array is read by a LIST object inspector
            loi = (ListObjectInspector) ois[0];

            // a list has an element type associated to it
            // elements must be structs for this UDF
            if( loi.getListElementObjectInspector().getCategory() != ObjectInspector.Category.STRUCT) {
                throw new UDFArgumentTypeException(0, "Argument 1"
                        + " of function " +  this.getClass().getCanonicalName() + " must be an array of structs " +
                        " but is an array of " + loi.getListElementObjectInspector().getCategory().name());
            }

            // store the object inspector for the elements
            elOi = (StructObjectInspector)loi.getListElementObjectInspector();

            // returns the same object inspector
            if(!(ois[1] instanceof WritableConstantStringObjectInspector)){
                throw new UDFArgumentTypeException( 1 , "Argument 2 String constant expected");
            }
            WritableConstantStringObjectInspector constantOI1 = (WritableConstantStringObjectInspector) ois[1];
            fieldName  = constantOI1.getWritableConstantValue().toString();

            if (!(ois[2] instanceof WritableConstantStringObjectInspector)){
                throw new UDFArgumentTypeException( 2, "Argument 3 should be String constant expected Value= SUM / MIN / MAX ");
            }
            WritableConstantStringObjectInspector constantOI2 = (WritableConstantStringObjectInspector) ois[2];
            operation  = constantOI2.getWritableConstantValue().toString();
            System.out.println("OPERATION : " + operation );
//            System.out.println((operation.equalsIgnoreCase("SUM") || operation.equalsIgnoreCase("MIN") || operation.equalsIgnoreCase("MAX")));
            if (!(operation.equalsIgnoreCase("SUM") || operation.equalsIgnoreCase("MIN") || operation.equalsIgnoreCase("MAX"))){
                throw new UDFArgumentTypeException( 2, "Argument 3 expected Value= SUM / MIN / MAX ");
            }
            if (operation.equalsIgnoreCase("SUM")){
                return (ObjectInspector) PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
            }
            else{
                return (ObjectInspector) elOi.getStructFieldRef(fieldName).getFieldObjectInspector();
            }
        }

        // to sort a list , we must supply our comparator
        public  class StructFieldComparator implements Comparator {
            StructField field;

            public StructFieldComparator(String fieldName) {
                field = elOi.getStructFieldRef(fieldName);
            }

            public Object pick(Object o) {
                return ObjectInspectorUtils.copyToStandardJavaObject(
                        elOi.getStructFieldData(o, field)
                        , field.getFieldObjectInspector()
                );
            }

            public int compare(Object o1, Object o2) {
                Object f1 = pick(o1);
                Object f2 = pick(o2);

                return ObjectInspectorUtils.compare(f1, field.getFieldObjectInspector(),
                        f2, field.getFieldObjectInspector());
            }
        }

        // factory method for cached comparators
        StructFieldComparator getComparator(String field) {
            if(!comparatorCache.containsKey(field)) {
                comparatorCache.put(field, new StructFieldComparator(field));
            }
            return comparatorCache.get(field);
        }

        @Override
        public Object evaluate(DeferredObject[] dos) throws HiveException {
            if(dos==null || dos.length != 3) {
                throw new HiveException("received " + (dos == null? "null" :
                        Integer.toString(dos.length) + " elements instead of 3"));
            }

            List al = loi.getList(dos[0].get());
            StructFieldComparator comparator = getComparator(dos[1].get().toString());

            if ("SUM".equalsIgnoreCase(operation)){
                return getSum(al, comparator);
            }
            else if ("MAX".equalsIgnoreCase(operation)){
                Object max = Collections.max(al, comparator);
                return comparator.pick(max);
            }
            else if ("MIN".equalsIgnoreCase(operation)){
                Object min = Collections.min(al, comparator);
                return comparator.pick(min);
            }

            return null;
        }

        @Override
        public String getDisplayString(String[] children) {
            return  (children == null? null : this.getClass().getCanonicalName() + "(" + children[0] + "," + children[1] + ")");
        }


        private Object getSum(List arrayList, StructFieldComparator comparator){
            double sum = 0.0;

                for (Object o : arrayList ) {
                    Object num = comparator.pick(o);
                    if (num instanceof Integer)
                        sum += ((Integer) num).doubleValue();
                    else if (num instanceof Double)
                        sum += (Double) num;
                    else if (num instanceof Float)
                        sum += ((Float) num).doubleValue();
                    else if (num instanceof Long)
                        sum += ((Long) num).doubleValue();
                    else
                        sum += 0;
                }
                return sum;

        }

    }

