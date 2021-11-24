package io.kyligence.jni.engine;

public class Chunk {
    public static class ColumnInfo {
        // The data stored in these two allocations need to maintain binary compatible. We can
        // directly pass this buffer to external components.
        private long nulls;
        private long data;

        // Only set if type is Array or Map.
        private long lengthData;
        private long offsetData;

        public ColumnInfo(long nulls, long data, long lengthData, long offsetData) {
            this.nulls = nulls;
            this.data = data;
            this.lengthData = lengthData;
            this.offsetData = offsetData;
        }

        public long getNulls() {
            return nulls;
        }

        public long getData() {
            return data;
        }

        public long getLengthData() {
            return lengthData;
        }

        public long getOffsetData() {
            return offsetData;
        }
    }

    private final ColumnInfo[] columns;
    private final long rowCount;

    public Chunk(ColumnInfo[] columns, long rowCount) {
        this.columns = columns;
        this.rowCount = rowCount;
    }

    public ColumnInfo[] getColumns() {
        return columns;
    }

    public long getRowCount() {
        return rowCount;
    }
}
