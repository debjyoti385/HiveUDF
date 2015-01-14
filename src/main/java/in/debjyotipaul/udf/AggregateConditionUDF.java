package in.debjyotipaul.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.LIST;


@Description(name = "array_struct_min_max_sum",
        value = "_FUNC_(array(struct1,struct2,...), string myfield) - "
                + "returns the passed array struct, ordered by the given field  " ,
        extended = "Example:\n"
                + "  > SELECT _FUNC_('SUM'/'MIN'/'MAX',Column,fieldName,conditionFieldName,'LT'/'GT'/'EQ'/'NE',value) FROM src LIMIT 1;\n"
                + " 'b' ")
public class AggregateConditionUDF extends GenericUDF {
    protected ObjectInspector[] argumentOIs;

    private ListObjectInspector loi;
    private StructObjectInspector elOi;
    private String operation;
    private String fieldName;
    private String[] conditionColNames ;
    private ConditionType[] conditionType;
    private Object[] conditionValue;
    private int numColumnz;

    Map<String,StructFieldComparator> comparatorCache = new HashMap<String,StructFieldComparator>();

    @Override
    public ObjectInspector initialize(ObjectInspector[] ois) throws UDFArgumentException {

        argumentOIs = ois;
        comparatorCache.clear();
        numColumnz = (ois.length - 3) / 3;
        conditionColNames = new String[numColumnz];
        conditionType = new ConditionType[numColumnz];
        conditionValue = new Object[numColumnz];

        return checkAndReadObjectInspectors(ois);
    }

    protected ObjectInspector checkAndReadObjectInspectors(ObjectInspector[] ois)
            throws UDFArgumentTypeException, UDFArgumentException {

        int numFields = ois.length;

        if(numFields % 3 != 0) {
            throw new UDFArgumentException("Arguments needed should be multiple of 3, found " + ois.length );
        }

        if (!(ois[0] instanceof WritableConstantStringObjectInspector)){
            throw new UDFArgumentTypeException( 0, "Argument 1 should be String constant expected Value= SUM / MIN / MAX ");
        }

        WritableConstantStringObjectInspector constantOI1 = (WritableConstantStringObjectInspector) ois[0];
        operation  = constantOI1.getWritableConstantValue().toString();

        System.out.println("OPERATION : " + operation );

        if (!(operation.equalsIgnoreCase("SUM") || operation.equalsIgnoreCase("MIN") || operation.equalsIgnoreCase("MAX") || operation.equalsIgnoreCase("COUNT"))){
            throw new UDFArgumentTypeException( 0, "Argument 3 expected Value= SUM / MIN / MAX / COUNT ");
        }

        // first argument must be a list/array
        if (! ois[1].getCategory().equals(LIST)) {
            throw new UDFArgumentTypeException(1, "Argument 2"
                    + " of function " + this.getClass().getCanonicalName() + " must be " + Constants.LIST_TYPE_NAME
                    + ", but " + ois[1].getTypeName()
                    + " was found.");
        }

        // a list/array is read by a LIST object inspector
        loi = (ListObjectInspector) ois[1];

        // a list has an element type associated to it
        // elements must be structs for this UDF
        if( loi.getListElementObjectInspector().getCategory() != ObjectInspector.Category.STRUCT) {
            throw new UDFArgumentTypeException(1, "Argument 2"
                    + " of function " +  this.getClass().getCanonicalName() + " must be an array of structs " +
                    " but is an array of " + loi.getListElementObjectInspector().getCategory().name());
        }

        // store the object inspector for the elements
        elOi = (StructObjectInspector)loi.getListElementObjectInspector();

        // returns the same object inspector
        if(!(ois[2] instanceof WritableConstantStringObjectInspector)){
            throw new UDFArgumentTypeException( 2 , "Argument 3 String constant expected");
        }
        WritableConstantStringObjectInspector constantOI2 = (WritableConstantStringObjectInspector) ois[2];
        fieldName  = constantOI2.getWritableConstantValue().toString();

        for (int f = 3,k=0; f < numFields; f=f+3,k++){
            if(!(ois[f] instanceof WritableConstantStringObjectInspector)){
                throw new UDFArgumentTypeException(f, f + " Key arguments must be a constant STRING " + ois[f].toString());
            }
            WritableConstantStringObjectInspector constantOI3 = (WritableConstantStringObjectInspector)ois[f];
            conditionColNames[k] = constantOI3.getWritableConstantValue().toString();

            if(!(ois[f + 1] instanceof WritableConstantStringObjectInspector)){
                throw new UDFArgumentTypeException(f + 1 , (f + 1) + " Key arguments must be a constant STRING " + ois[f + 1].toString());
            }
            WritableConstantStringObjectInspector constantOI4 = (WritableConstantStringObjectInspector)ois[f+1];
            conditionType[k] = ConditionType.valueOf(constantOI4.getWritableConstantValue().toString());



            ObjectInspector valueOI = ois[f + 2];
            if(!(ois[f + 2] instanceof ConstantObjectInspector)){
                throw new UDFArgumentTypeException(f + 2, "Value arguments must be primitives " + ois[f + 2].toString());
            }
            conditionValue[k]  = ((ConstantObjectInspector) valueOI).getWritableConstantValue();

//            System.out.println(conditionColNames[k]);
//            System.out.println(conditionType[k]);
//            System.out.println(conditionValue[k]);

        }


        if (operation.equalsIgnoreCase("COUNT")){
            return (ObjectInspector) PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        }
        else{
            return (ObjectInspector) PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
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

        public Class getFieldJavaClass(){
            return PrimitiveObjectInspectorUtils.getJavaPrimitiveClassFromObjectInspector(
                    field.getFieldObjectInspector()
            );
        }
    }

    // factory method for cached comparators
    StructFieldComparator getComparator(String field) {
        if(!comparatorCache.containsKey(field)) {
            comparatorCache.put(field, new StructFieldComparator(field));
        }
        return comparatorCache.get(field);
    }

    public enum ConditionType{
        LT, GT, EQ, NE, LTE, GTE;
    }


    @Override
    public Object evaluate(DeferredObject[] dos) throws HiveException {
        if(dos==null || (dos.length % 3) != 0) {
            throw new HiveException("received " + (dos == null? "null" :
                    Integer.toString(dos.length) + " elements instead of multiple of 3"));
        }

        List al = loi.getList(dos[1].get());
        StructFieldComparator comparator = getComparator(fieldName);

        StructFieldComparator[] conditionColComparator = new StructFieldComparator[numColumnz];
        for (int i = 0 ; i< numColumnz ; i++){
                conditionColComparator[i] = getComparator(conditionColNames[i]);
        }

        if ("SUM".equalsIgnoreCase(operation)){
//            System.out.println("DOING " + operation);
            return getSum(al, comparator, conditionColComparator);
        }
        else if ("MAX".equalsIgnoreCase(operation)){
//            System.out.println("DOING " + operation);
            return getMax(al, comparator, conditionColComparator);
        }
        else if ("MIN".equalsIgnoreCase(operation)){
//            System.out.println("DOING " + operation);
            return getMin(al, comparator, conditionColComparator);
        }

        else if ("COUNT".equalsIgnoreCase(operation)){
//            System.out.println("DOING " + operation);
            return getCount(al, comparator, conditionColComparator);
        }

        return null;
    }

    @Override
    public String getDisplayString(String[] children) {
        return  (children == null? null : this.getClass().getCanonicalName() + "(" + children[0] + "," + children[1] + ")");
    }


    private Object getMin(List arrayList, StructFieldComparator comparator, StructFieldComparator[] conditionColComparator ){
        double min = Double.MAX_VALUE;
        for (Object o : arrayList ) {
            Object num = comparator.pick(o);

            boolean isValid = validate(o,conditionColComparator);

            if (isValid){
                if (num instanceof Integer){
                    if( (min - ((Integer) num).doubleValue()) > 0 ){
                        min = ((Integer) num).doubleValue();
                    }
                }
                else if (num instanceof Double){
                    if ( (min - ((Double) num).doubleValue()) > 0 ){
                        min = ((Double) num).doubleValue();
                    }
                }
                else if (num instanceof Float){
                    if ( (min - ((Float) num).doubleValue()) > 0 ){
                        min = ((Float) num).doubleValue();
                    }
                }
                else if (num instanceof Long){
                    if ( (min - ((Long) num).doubleValue()) > 0 ){
                        min = ((Long) num).doubleValue();
                    }
                }
            }
        }
        return min;

    }

    private Object getMax(List arrayList, StructFieldComparator comparator, StructFieldComparator[] conditionColComparator ){
        double max = Double.MIN_VALUE;
        for (Object o : arrayList ) {
            Object num = comparator.pick(o);

            boolean isValid = validate(o,conditionColComparator);

            if (isValid){
                if (num instanceof Integer)
                    max = (((Integer) num).doubleValue() - max > 0) ? ((Integer) num).doubleValue() : max ;
                else if (num instanceof Double)
                    max = (((Double) num).doubleValue() - max > 0) ? ((Integer) num).doubleValue() : max ;
                else if (num instanceof Float)
                    max = (((Float) num).doubleValue() - max > 0) ? ((Integer) num).doubleValue() : max ;
                else if (num instanceof Long)
                    max = (((Long) num).doubleValue() - max > 0) ? ((Integer) num).doubleValue() : max ;
            }
        }
        return max;
    }


    private Object getSum(List arrayList, StructFieldComparator comparator, StructFieldComparator[] conditionColComparator ){
        double sum = 0.0;

        for (Object o : arrayList ) {
            Object num = comparator.pick(o);

            boolean isValid = validate(o,conditionColComparator);

            if (isValid){
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
        }
        return sum;

    }

    private Object getCount(List arrayList, StructFieldComparator comparator, StructFieldComparator[] conditionColComparator ){
        int count = 0;
        for (Object o : arrayList ) {
            Object num = comparator.pick(o);

            boolean isValid = validate(o,conditionColComparator);

            if (isValid){
                count++;
            }
        }
        return count;

    }

    private boolean compare(int object, int i){

        switch(conditionType[i]){
            case LT:    if ( object < (Integer) conditionValue[i]){
                            return true;
                        }
                break;
            case GT:    if ( object > (Integer) conditionValue[i]){
                            return true;
                        }
                break;
            case EQ:    if ( object == (Integer) conditionValue[i]){
                            return true;
                        }
                break;
            case NE:    if ( object != (Integer) conditionValue[i]){
                            return true;
                        }
                break;
            case LTE:    if ( object <= (Integer) conditionValue[i]){
                return true;
            }
                break;
            case GTE:    if ( object >= (Integer) conditionValue[i]){
                return true;
            }
                break;
        }
        return false;
    }

    private boolean compare(Double object, int i){

        switch(conditionType[i]){
            case LT:    if ( object < (Double) conditionValue[i]){
                return true;
            }
                break;
            case GT:    if ( object > (Double) conditionValue[i]){
                return true;
            }
                break;
            case EQ:    if ( object == (Double) conditionValue[i]){
                return true;
            }
                break;
            case NE:    if ( object != (Double) conditionValue[i]){
                return true;
            }
                break;
            case LTE:    if ( object <= (Double) conditionValue[i]){
                return true;
            }
                break;
            case GTE:    if ( object >= (Double) conditionValue[i]){
                return true;
            }
                break;
        }
        return false;
    }

    private boolean compare(Long object, int i){

        switch(conditionType[i]){
            case LT:    if ( object < (Long) conditionValue[i]){
                return true;
            }
                break;
            case GT:    if ( object > (Long) conditionValue[i]){
                return true;
            }
                break;
            case EQ:    if ( object == (Long) conditionValue[i]){
                return true;
            }
                break;
            case NE:    if ( object != (Long) conditionValue[i]){
                return true;
            }
                break;
            case LTE:    if ( object <= (Long) conditionValue[i]){
                return true;
            }
                break;
            case GTE:    if ( object >= (Long) conditionValue[i]){
                return true;
            }
                break;
        }
        return false;
    }

    private boolean compare(Float object, int i){

        switch(conditionType[i]){
            case LT:    if ( object < (Float) conditionValue[i]){
                return true;
            }
                break;
            case GT:    if ( object > (Float) conditionValue[i]){
                return true;
            }
                break;
            case EQ:    if ( object == (Float) conditionValue[i]){
                return true;
            }
                break;
            case NE:    if ( object != (Float) conditionValue[i]){
                return true;
            }
                break;
            case LTE:    if ( object <= (Float) conditionValue[i]){
                return true;
            }
                break;
            case GTE:    if ( object >= (Float) conditionValue[i]){
                return true;
            }
                break;
        }
        return false;
    }

    private boolean compare(String object, int i){

        switch(conditionType[i]){
            case LT:    if ( object.compareTo((String)conditionValue[i].toString()) < 0 ){
                return true;
            }
                break;
            case GT:    if (object.compareTo((String)conditionValue[i].toString()) > 0 ){
                return true;
            }
                break;
            case EQ:    if ( object.compareTo((String)conditionValue[i].toString()) == 0 ){
                return true;
            }
                break;
            case NE:    if ( object.compareTo((String)conditionValue[i].toString()) != 0  ){
                return true;
            }
                break;
            case LTE:    if ( object.compareTo((String)conditionValue[i].toString()) <= 0 ){
                return true;
            }
                break;
            case GTE:    if (object.compareTo((String)conditionValue[i].toString()) >= 0 ){
                return true;
            }
        }
        return false;
    }


    private boolean validate(Object o, StructFieldComparator[] conditionColComparator){

//        System.out.println("INSIDE validate()");
        boolean isValid = true;
        for (int i=0; i<numColumnz; i++ ){
            Object objectValue = conditionColComparator[i].pick(o);
            if (objectValue instanceof Integer)
                isValid = isValid && compare((Integer) objectValue, i );
            else if (objectValue instanceof Double)
                isValid = isValid && compare((Double) objectValue, i );
            else if (objectValue instanceof Float)
                isValid = isValid && compare((Float) objectValue, i );
            else if (objectValue instanceof Long)
                isValid = isValid && compare((Long) objectValue, i );
            else
                isValid = isValid && compare(objectValue.toString(), i );
        }
        return isValid;
    }

}

