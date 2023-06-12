package com.github.thermo911.parquet;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import com.github.thermo911.conf.Config;
import com.github.thermo911.data.Trip;
import com.github.thermo911.data.TripDataset;
import com.github.thermo911.stat.MaxLongAccumulator;
import com.github.thermo911.stat.MinLongAccumulator;
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

        List<TripDatasetIndex.MinMax> rowGroupsMinMax = newRowGroupIndicesStream()
                .mapToObj(i -> new TripDatasetIndex.MinMax(
                        mins.get(i).getResult(),
                        maxs.get(i).getResult()
                ))
                .toList();

        tripDatasetIndex = new TripDatasetIndex(rowGroupsMinMax);
    }

    private IntStream newRowGroupIndicesStream() {
        return IntStream.range(0, rowGroupsCount);
    }

    @Override
    public Iterator<Trip> trips(LocalDateTime start, LocalDateTime end) {
        long startMillis = start.toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
        long endMillis = end.toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
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

        private PageReadStore currRowGroup;
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
            if (currRowGroup == null && !nextRowGroup()) {
                return false;
            }

            if (nextTrip == null) {
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
                if (currRow == currRowsCount) {
                    // end of current row group
                    currRowGroup = null;
                }
            }

            return nextTrip != null;
        }

        private boolean nextRowGroup() {
            currRowGroupIndex = null;
            currRowGroup = null;
            currRow = 0;
            currRecordReader = null;
            currRowsCount = 0;

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
