/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static com.persistit.Configuration.*;
import java.util.Properties;

import org.junit.Test;

import com.persistit.Configuration.BufferMemorySpecification;
import com.persistit.unit.PersistitUnitTestCase;

public class ConfigurationTest extends PersistitUnitTestCase {

    @Test
    public void testStaticMethods() throws Exception {
        assertEquals(1024, bufferSizeFromPropertyName("buffer.memory.1024"));
        assertEquals(2048, bufferSizeFromPropertyName("buffer.memory.2048"));
        assertEquals(4096, bufferSizeFromPropertyName("buffer.memory.4096"));
        assertEquals(8192, bufferSizeFromPropertyName("buffer.memory.8192"));
        assertEquals(16384, bufferSizeFromPropertyName("buffer.memory.16384"));
        assertEquals(-1, bufferSizeFromPropertyName("buffer.memory.1023"));
        assertEquals(-1, bufferSizeFromPropertyName("buffer.memory.notanumber"));
        assertEquals(-1, bufferSizeFromPropertyName("buffer.fricostat"));

        int size = 1024;
        assertEquals(-1, bufferSizeIndex(1023));
        assertEquals(-1, bufferSizeIndex(0));
        for (int index = 0; size <= 16384; index++) {
            assertEquals(index, bufferSizeIndex(size));
            size *= 2;
        }

        assertEquals("3G", displayableLongValue(3L * 1024L * 1024L * 1024L));
        assertEquals("1M", displayableLongValue(1024 * 1024));
        assertEquals("1000005", displayableLongValue(1000005L));
        assertEquals("10000000003", displayableLongValue(10000000003L));
        assertEquals(true, parseBooleanValue("z", "true"));
        assertEquals(true, parseBooleanValue("z", "TRUE"));
        assertEquals(false, parseBooleanValue("z", "false"));
        assertEquals(false, parseBooleanValue("z", "FALSE"));

        try {
            parseBooleanValue("z", "Neither True nor False");
            fail("Exception not thrown");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            parseBooleanValue("z", null);
            fail("Exception not thrown");
        } catch (IllegalArgumentException e) {
            // expected
        }

        assertEquals(1024L, parseLongProperty("z", "1024"));
        assertEquals(1024L, parseLongProperty("z", "1K"));
        assertEquals(1024L * 1024L, parseLongProperty("z", "1M"));
        assertEquals(1024L * 1024L * 1024L, parseLongProperty("z", "1G"));
        assertEquals(1024L * 1024L * 1024L * 1024L, parseLongProperty("z", "1T"));

        try {
            parseLongProperty("z", "not a number");
            fail("Exception not thrown");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            parseLongProperty("z", null);
            fail("Exception not thrown");
        } catch (IllegalArgumentException e) {
            // expected
        }

        assertEquals(0, Float.compare(0.25f, parseFloatProperty("z", "0.25")));
        assertEquals(0, Float.compare(0.0f, parseFloatProperty("z", "0")));
        assertEquals(0, Float.compare(1.0f, parseFloatProperty("z", "1")));

        try {
            parseFloatProperty("z", "not a number");
            fail("Exception not thrown");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            parseFloatProperty("z", null);
            fail("Exception not thrown");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testBufferMemorySpecifications() throws Exception {
        Configuration configuration = new Configuration();
        configuration.parseBufferMemorySpecification("buffer.count.8192", "100K");
        expectBms(configuration,8192, 102400, 102400, 0, Long.MAX_VALUE, 0, 1.0f);
        configuration.parseBufferMemorySpecification("buffer.memory.8192", ",100M,24M");
        expectBms(configuration, 8192, 0, Integer.MAX_VALUE, 0, 100 * MEGA, 24 * MEGA, 1.0f);
    }
    
    private void expectBms(Configuration configuration, int bufferSize, int minimumCount, int maximumCount, long minimumMem, long maximumMem, long reservedMem, float fraction) {
        for (int index = 0; index < BUFFER_SIZES.length; index++) {
            int size = BUFFER_SIZES[index];
            if (size == bufferSize) {
                assertEquals("Buffer size", bufferSize, configuration.getBuffers()[index].getBufferSize());
                assertEquals("Minimum count", minimumCount, configuration.getBuffers()[index].getMinimumCount());
                assertEquals("Maximum count", maximumCount, configuration.getBuffers()[index].getMaximumCount());
                assertEquals("Minimum mem", minimumMem, configuration.getBuffers()[index].getMinimumMemory());
                assertEquals("Maximum mem", maximumMem, configuration.getBuffers()[index].getMaximumMemory());
                assertEquals("Reserved mem", reservedMem, configuration.getBuffers()[index].getReservedMemory());
                assertEquals("Fraction", 0, Float.compare(fraction, configuration.getBuffers()[index].getFraction()));
            } else {
                assertEquals("Buffer size", -1, configuration.getBuffers()[index].getBufferSize());
            }
        }
    }

    @Test
    public void testLoadPropertiesBufferSpecifications() throws Exception {
        final Properties properties = new Properties();
        BufferMemorySpecification bms;
        properties.put("buffer.count.1024", "500");
        bms = testLoadPropertiesBufferSpecificationsHelper(properties).getBuffers()[bufferSizeIndex(1024)];
        assertEquals(500, bms.getMaximumCount());
        properties.put("buffer.memory.16384", "1M,1G,128M,0.6");
        bms = testLoadPropertiesBufferSpecificationsHelper(properties).getBuffers()[Configuration
                .bufferSizeIndex(16384)];
        assertEquals(16384, bms.getBufferSize());
        assertEquals(MEGA, bms.getMinimumMemory());
        assertEquals(GIGA, bms.getMaximumMemory());
        assertEquals(128 * MEGA, bms.getReservedMemory());
        assertEquals(0, Float.compare(0.6f, bms.getFraction()));
        properties.put("buffer.memory.1024", "1M");
        try {
            testLoadPropertiesBufferSpecificationsHelper(properties).getBuffers()[0].getMaximumCount();
            fail("Exception not thrown");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private Configuration testLoadPropertiesBufferSpecificationsHelper(final Properties properties) throws Exception {
        Configuration configuration = new Configuration();
        configuration.merge(properties);
        configuration.loadPropertiesBufferSpecifications();
        return configuration;
    }
}
