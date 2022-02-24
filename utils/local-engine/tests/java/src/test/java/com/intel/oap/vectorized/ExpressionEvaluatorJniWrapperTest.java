package com.intel.oap.vectorized;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExpressionEvaluatorJniWrapperTest {
    @BeforeClass
    public void setup() {
        System.load();
    }

        @Test
    public void testTPCHQ6() {
        ExpressionEvaluatorJniWrapper jniWrapper = new ExpressionEvaluatorJniWrapper();

    }

}
