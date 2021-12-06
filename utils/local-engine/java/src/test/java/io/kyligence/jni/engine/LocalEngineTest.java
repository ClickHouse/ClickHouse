package io.kyligence.jni.engine;


import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.charset.StandardCharsets;

@RunWith(JUnit4.class)
//@Ignore
public class LocalEngineTest {

    @Before
    public void setup() {
        System.out.println("start load");
        System.load("/home/kyligence/Documents/code/ClickHouse/cmake-build-debug/utils/local-engine/liblocal_engine_jnid.so");
        System.out.println("load success");
    }

    @Test
    public void testLocalEngine() throws Exception{
        String plan = IOUtils.resourceToString("/plan.txt", StandardCharsets.UTF_8);
        LocalEngine localEngine = new LocalEngine(plan.getBytes(StandardCharsets.UTF_8));
        localEngine.execute();
        Assert.assertTrue(localEngine.hasNext());
        SparkRowInfo data = localEngine.next();
        Assert.assertTrue(data.memoryAddress > 0);
        Assert.assertEquals(150, data.offsets.length);
        UnsafeRow row  = new UnsafeRow(5);
        row.pointTo(null, data.memoryAddress + data.offsets[5], (int) data.lengths[5]);
        Assert.assertEquals(5.4, row.getDouble(2), 0.00001);
        Assert.assertEquals(0, row.getInt(4));
    }
}
