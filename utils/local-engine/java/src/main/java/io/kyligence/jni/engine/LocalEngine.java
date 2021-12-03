package io.kyligence.jni.engine;

import java.io.Closeable;
import java.io.IOException;

public class LocalEngine implements Closeable {
    public static native long test(int a, int b);

    public static native void initEngineEnv();

    public static void main(String[] args) throws InterruptedException {
        System.out.println("start load library");
        System.load("/home/kyligence/Documents/code/ClickHouse/cmake-build-debug/utils/local-engine/liblocal_engine_jnid.so");
        System.out.println("start in java");
        long result = test(1, 2);
        System.out.println(result);
    }

    private long nativeExecutor;
    private byte[] plan;

    public LocalEngine(byte[] plan) {
        this.plan = plan;
    }

    public native void execute();

    public native boolean hasNext();

    public native SparkRowInfo next();


    @Override
    public native void close() throws IOException;
}
