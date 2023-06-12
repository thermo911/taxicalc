package com.github.thermo911;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.github.thermo911.data.Trip;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

public class ParquetUtils {

    private static final Schema SCHEMA;
    private static final String SCHEMA_LOCATION = "src/test/resources/data/schema.avsc";

    static {
        try (InputStream inStream = new FileInputStream(SCHEMA_LOCATION)) {
            SCHEMA = new Schema.Parser().parse(IOUtils.toString(inStream, "UTF-8"));
        } catch (IOException e) {
            throw new RuntimeException("Can't read SCHEMA file from" + SCHEMA_LOCATION, e);
        }
    }

    public static void writeTripsToParquet(List<Trip> trips, String fileToWrite) throws IOException {
        var configuration = new Configuration();
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(
                        HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(fileToWrite), configuration)
                )
                .withSchema(SCHEMA)
                .withConf(configuration)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            for (Trip trip : trips) {
                GenericData.Record record = toRecord(trip);
                writer.write(record);
            }
        }
    }

    private static GenericData.Record toRecord(Trip trip) {
        var record = new GenericData.Record(SCHEMA);
        record.put("tpep_pickup_datetime", trip.pickupTimeMs());
        record.put("tpep_dropoff_datetime", trip.dropoffTimeMs());
        record.put("passenger_count", (double) trip.passengerCount());
        record.put("trip_distance", trip.tripDistance());
        return record;
    }
}
