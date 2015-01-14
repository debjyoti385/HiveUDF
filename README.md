Hive UDFs
________________________

## UDFs

1. Aggregate UDF 
```
Finds MIN, MAX and SUM from array of Struct Objects based on a field.
```
2. Sort UDF
```
Returns sorted array of Struct objects for an array of Struct Objects based on a field.
```

3. Date UDF (for Lookup on Date dimension, Data Warehousing concept)
```
Returns date key field, e.g. __FUNC__('2014-04-05T01:30:34Z') ISO 8601 format, __FUNC__('2014-04-05 01:30:34') and __FUNC__(UNIX_TIMESTAMP('2014-04-05 01:30:34'))  returns 20140405
```

4. Time UDF (for Lookup on Time dimension, Data Warehousing concept)
```
Returns date key field, e.g. __FUNC__('2014-04-05T01:30:34Z') ISO 8601 format, __FUNC__('2014-04-05 01:30:34') and __FUNC__(UNIX_TIMESTAMP('2014-04-05 01:30:34'))  returns 130
```

5. Contains UDF
```
__FUNC__(List<String>, String)
returns true or false accordingly.
```


## Usage and execution:
____________________

```shell
$ mvn package assembly:single
$ hive
hive> ADD JAR /path/to/hiveUDF-1.0-SNAPSHOT-jar-with-dependencies.jar;
hive> CREATE TEMPORARY FUNCTION addHello as 'in.debjyotipaul.udf.HiveUDFSimpleSample';
hive> SELECT addHello(name) from table;
```



