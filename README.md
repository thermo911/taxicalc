# taxicalc

[Apache Parquet](https://parquet.apache.org/docs/) files reading/writing
and having fun :)

## How to run example
1. Download Parquet files with
   ```shell
   bash load-datasets.sh
   ```
2. Enjoy!

## Tools
### Apache Parquet files reading
- `org.apache.parquet:parquet-avro` - actual reading
- `org.apache.hadoop:hadoop-common` - some useful utils for working with file
- Avro/Parquet Viewer Intellij Idea plugin

### Testing
- JUnit 5

## Applied optimizations
### Data projection
As Apache Parquet is column-oriented format it allows not to read all columns during 
scan but ones participating in "query" only. In this project data projection is performed
in following way:
```java
private MessageType project(MessageType schema) {
    List<Type> types = schema.getColumns().stream()
            .map(columnDescriptor -> (Type) columnDescriptor.getPrimitiveType())
            .filter(type -> Config.COLUMN_NAMES.contains(type.getName()))
            .toList();
    return new MessageType("Trip projection", types);
}
...
MessageType schema = project(reader.getFooter().getFileMetaData().getSchema());
reader.setRequestedSchema(schema);
```

### Trip dataset index
Instead of scanning all row groups in Parquet file it is reasonable to scan only those
that could contain data being searched. Here we have interface `TripDatasetIndex` and
its trivial implementation `DummyTripDatasetIndex`.

During dataset creation all row groups in Parquet file are scanned in order to calculate 
minimum `tpep_pickup_datetime` and maximum `tpep_dropoff_datetime` for each row group.

Once index is built it can help us to eliminate row groups not containing searched data.

## Challenges
Unfortunately, Apache Parquet file-reading tools documentation leaves much to be desired.
It was really time-consuming to figure out how to do something.

## Possible improvements
### Records reading
It could be nice to use `FilteringRecordMaterializer` instead of simple `RecordMaterializer` 
to filter out "bad" data on record reading level. In this project `TripConverter` 
(the implementation of `RecordMaterializer`) was used which leaded to code like this:
```java
// TripConverter

public static final Trip BROKEN_TRIP = new Trip(-1, -1.0, -1L, -1L);
...
if (bad Trip record) {
    return BROKEN_TRIP;
}
```
```java
// ParquetTripDataset.TripIterator

if (trip == TripConverter.BROKEN_TRIP) {
    continue;
}
```

### Statistics usage
It seems like `org.apache.parquet.column.statistics.Statistics` could be used in scans
to improve performance. However, all my attempts to figure out how to do this were failed (my bad).

### Files preprocessing
Working with raw Parquet files we cannot make any assumptions about order of records, ranges of values etc.
so we are not able to make better optimizations.

#### Records ordering
In our case, if records were ordered by `tpep_pickup_datetime` and `tpep_dropoff_datetime`
it would be possible to optimize searching of trips by time.

#### Increasing Parquet files granularity
After records ordering we can split ordered records into more row groups so that trips start/end from 
each row group fit sequential time segments. Once granularity is increased we can use index more precisely.

### Scans parallelization
We can work with different files in parallel for better resource utilization.

## Tests
Testing is not the best part of this project. There are only 2 tests added:
- `ParquetTripDatasetTest` checks that `ParquetTripDataset` reads and searches trips correctly
- `AverageDistancesImplTest` chacks that `AverageDistancesImpl::getAverageDistances` works correctly for specified data

What else must be checked on "production level":
1. Proper work of indexing mechanism
2. Some edge cases
3. Performance
