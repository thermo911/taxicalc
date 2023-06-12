package com.github.thermo911.parquet;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import com.github.thermo911.conf.Config;
import com.github.thermo911.data.Trip;
import com.github.thermo911.data.TripDataset;
import com.github.thermo911.stat.MaxLongAccumulator;
import com.github.thermo911.stat.MinLongAccumulator;
import com.github.thermo911.util.Utils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * Class for accessing trip data from Apache Parquet file.
 */
public class ParquetTripDataset implements TripDataset {
    private final ParquetFileReader reader;
    private final MessageColumnIO columnIO;
    private final RecordMaterializer<Trip> converter;
    private final int rowGroupsCount;

    private TripDatasetIndex tripDatasetIndex;

    /**
     * Creates dataset based on Apache Parquet file located on specified path.
     * @param filePath path to Apache Parquet file
     */
    public ParquetTripDataset(String filePath) {
        try {
            reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), Config.HADOOP_CONFIG));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        MessageType schema = project(reader.getFooter().getFileMetaData().getSchema());
        reader.setRequestedSchema(schema);
        columnIO = new ColumnIOFactory().getColumnIO(schema);
        converter = new TripConverter(schema);
        rowGroupsCount = reader.getRowGroups().size();

        initDatasetIndex();
    }

    private MessageType project(MessageType schema) {
        List<Type> types = schema.getColumns().stream()
                .map(columnDescriptor -> (Type) columnDescriptor.getPrimitiveType())
                .filter(type -> Config.COLUMN_NAMES.contains(type.getName()))
                .toList();
        return new MessageType("Trip projection", types);
    }

    private void initDatasetIndex() {
        var trips = new TripDatasetIterator(Long.MIN_VALUE, Long.MAX_VALUE, newRowGroupIndicesStream().iterator());
        List<MinLongAccumulator> mins = new ArrayList<>();
        List<MaxLongAccumulator> maxs = new ArrayList<>();
        newRowGroupIndicesStream().forEach(i -> {
            mins.add(new MinLongAccumulator());
            maxs.add(new MaxLongAccumulator());
        });

        while (trips.hasNext()) {
            Trip trip = trips.next();
            int currRowGroupIndex = trips.currRowGroupIndex;
            mins.get(currRowGroupIndex).accept(trip.pickupTimeMs());
            maxs.get(currRowGroupIndex).accept(trip.dropoffTimeMs());
        }

        List<DummyTripDatasetIndex.MinMax> rowGroupsMinMax = newRowGroupIndicesStream()
                .mapToObj(i -> new DummyTripDatasetIndex.MinMax(
                        mins.get(i).getResult(),
                        maxs.get(i).getResult()
                ))
                .toList();

        tripDatasetIndex = new DummyTripDatasetIndex(rowGroupsMinMax);
    }

    private IntStream newRowGroupIndicesStream() {
        return IntStream.range(0, rowGroupsCount);
    }

    @Override
    public Iterator<Trip> trips(LocalDateTime start, LocalDateTime end) {
        if (start.isAfter(end)) {
            return Collections.emptyIterator();
        }

        long startMillis = Utils.toEpochMillis(start);
        long endMillis = Utils.toEpochMillis(end);
        Iterator<Integer> rowGroupsIndices = tripDatasetIndex.getRowGroupIndices(startMillis, endMillis).iterator();
        return new TripDatasetIterator(startMillis, endMillis, rowGroupsIndices);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private class TripDatasetIterator implements Iterator<Trip> {
        private final long startTimeMillis;
        private final long endTimeMillis;

        private final Iterator<Integer> rowGroupIndices;
        private Integer currRowGroupIndex;

        private RecordReader<Trip> currRecordReader;
        private long currRowsCount;
        private long currRow;

        private Trip nextTrip;

        private TripDatasetIterator(long startTimeMillis, long endTimeMillis, Iterator<Integer> rowGroupIndices) {
            this.startTimeMillis = startTimeMillis;
            this.endTimeMillis = endTimeMillis;
            this.rowGroupIndices = rowGroupIndices;
        }

        @Override
        public Trip next() {
            if (hasNext()) {
                Trip t = nextTrip;
                nextTrip = null;
                return t;
            }
            throw new NoSuchElementException("No trip!");
        }

        @Override
        public boolean hasNext() {
            if (nextTrip != null) {
                return true;
            }
            if (currRow == currRowsCount && !readNextRowGroup()) {
                return false;
            }

            while (currRow < currRowsCount) {
                Trip trip = currRecordReader.read();
                currRow++;
                if (trip == TripConverter.BROKEN_TRIP) {
                    continue;
                }
                if (startTimeMillis <= trip.pickupTimeMs() && trip.dropoffTimeMs() <= endTimeMillis) {
                    nextTrip = trip;
                    break;
                }
            }

            return nextTrip != null;
        }

        private boolean readNextRowGroup() {
            currRowGroupIndex = null;
            currRow = 0;
            currRecordReader = null;
            currRowsCount = 0;

            PageReadStore currRowGroup = null;
            if (rowGroupIndices.hasNext()) {
                try {
                    currRowGroupIndex = rowGroupIndices.next();
                    currRowGroup = reader.readRowGroup(currRowGroupIndex);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            if (currRowGroup != null) {
                currRecordReader = columnIO.getRecordReader(currRowGroup, converter);
                currRowsCount = currRowGroup.getRowCount();
            }
            return currRowGroup != null;
        }
    }
}
