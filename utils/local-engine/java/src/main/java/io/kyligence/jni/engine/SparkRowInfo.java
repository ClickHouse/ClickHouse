package io.kyligence.jni.engine;

public class SparkRowInfo {
    public long[] offsets;
    public long[] lengths;
    public long memoryAddress;
    public long column_number;

    public SparkRowInfo(long[] offsets, long[] lengths, long memoryAddress, long column_number) {
        this.offsets = offsets;
        this.lengths = lengths;
        this.memoryAddress = memoryAddress;
        this.column_number = column_number;
    }
}
