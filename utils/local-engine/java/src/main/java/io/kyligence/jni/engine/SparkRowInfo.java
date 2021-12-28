package io.kyligence.jni.engine;

public class SparkRowInfo {
    public long[] offsets;
    public long[] lengths;
    public long memoryAddress;
    public long fieldsNum;

    public SparkRowInfo(long[] offsets, long[] lengths, long memoryAddress, long fieldsNum) {
        this.offsets = offsets;
        this.lengths = lengths;
        this.memoryAddress = memoryAddress;
        this.fieldsNum = fieldsNum;
    }
}
