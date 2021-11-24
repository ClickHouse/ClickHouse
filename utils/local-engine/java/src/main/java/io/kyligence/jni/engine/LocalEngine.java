package io.kyligence.jni.engine;

public class LocalEngine {
    public static native long test(int a, int b);

    public static void main(String[] args) throws InterruptedException {
        System.out.println("start load library");
        System.load("/home/kyligence/Documents/code/ClickHouse/cmake-build-debug/utils/local-engine/liblocal_engine_jnid.so");
        System.out.println("start in java");
        long result = test(1, 2);
        System.out.println(result);
    }
}
