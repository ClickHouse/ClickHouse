package io.kyligence.jni.engine;


import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.charset.StandardCharsets;

@RunWith(JUnit4.class)
public class LocalEngineTest {

    @Before
    public void setup() {
        System.load("/home/kyligence/Documents/code/ClickHouse/cmake-build-debug/utils/local-engine/liblocal_engine_jnid.so");

    }

    @Test
    public void testLocalEngine() throws Exception{
        String plan = IOUtils.resourceToString("/plan.txt", StandardCharsets.UTF_8);
        LocalEngine localEngine = new LocalEngine(plan.getBytes(StandardCharsets.UTF_8));
        localEngine.execute();
        Assert.assertTrue(localEngine.hasNext());
        SparkRowInfo data = localEngine.next();
        Assert.assertEquals(150, data.offsets.length);
    }
}
