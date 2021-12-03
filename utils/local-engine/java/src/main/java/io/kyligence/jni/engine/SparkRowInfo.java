package io.kyligence.jni.engine;

public class SparkRowInfo {
    public long[] offsets;
    public long[] lengths;
    public long memoryAddress;

    public SparkRowInfo(long[] offsets, long[] lengths, long memoryAddress) {
        this.offsets = offsets;
        this.lengths = lengths;
        this.memoryAddress = memoryAddress;
    }
}
