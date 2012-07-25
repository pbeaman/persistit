/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit;

import static com.persistit.Configuration.GIGA;
import static com.persistit.Configuration.MEGA;
import static com.persistit.Configuration.bufferSizeFromPropertyName;
import static com.persistit.Configuration.checkBufferSize;
import static com.persistit.Configuration.displayableLongValue;
import static com.persistit.Configuration.parseBooleanValue;
import static com.persistit.Configuration.parseFloatProperty;
import static com.persistit.Configuration.parseLongProperty;
import static com.persistit.Configuration.validBufferSizes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

import com.persistit.Configuration.BufferPoolConfiguration;

public class ConfigurationTest extends PersistitUnitTestCase {

    private final static String RESOURCE_NAME = "com.persistit.ConfigurationTest";

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

        try {
            checkBufferSize(1023);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            checkBufferSize(0);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            // expected
        }

        for (int size : validBufferSizes()) {
            checkBufferSize(size);
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
    public void testBufferMemoryConfigurations() throws Exception {
        Configuration configuration = new Configuration();
        BufferPoolConfiguration bpc = configuration.getBufferPoolMap().get(8192);
        bpc.parseBufferCount(8192, "buffer.count.8192", "100K");
        expectBPConfig(configuration, 8192, 102400, 102400, 0, Long.MAX_VALUE, 0, 1.0f);
        bpc.parseBufferMemory(8192, "buffer.memory.8192", ",100M,24M");
        expectBPConfig(configuration, 8192, 0, Integer.MAX_VALUE, 0, 100 * MEGA, 24 * MEGA, 1.0f);
        bpc.parseBufferMemory(8192, "buffer.memory.8192", "1M,100M,24M,0.623f");
        expectBPConfig(configuration, 8192, 0, Integer.MAX_VALUE, 1 * MEGA, 100 * MEGA, 24 * MEGA, 0.623f);
        expectFail(8192, bpc, "buffer.memory.8192", "2M,2M,24M,0.623f");
        expectFail(8192, bpc, "buffer.memory.8192", "1M,200M,24M,1.1f");
        expectFail(8192, bpc, "buffer.count.8192", "not a number");
        expectFail(8192, bpc, "buffer.memory.8192", "2M,2M,24M,not a number");
    }

    private void expectFail(final int bufferSize, final BufferPoolConfiguration bpc, final String propertyName,
            final String propertyValue) {
        try {
            if (propertyName.contains("memory")) {
                bpc.parseBufferMemory(bufferSize, propertyName, propertyValue);
            } else if (propertyName.contains("count")) {
                bpc.parseBufferCount(bufferSize, propertyName, propertyValue);
            }
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private void expectBPConfig(Configuration configuration, int bufferSize, int minimumCount, int maximumCount,
            long minimumMem, long maximumMem, long reservedMem, float fraction) {
        for (int size : validBufferSizes()) {
            BufferPoolConfiguration bpc = configuration.getBufferPoolMap().get(size);
            if (size == bufferSize) {
                assertEquals("Buffer size", bufferSize, bpc.getBufferSize());
                assertEquals("Minimum count", minimumCount, bpc.getMinimumCount());
                assertEquals("Maximum count", maximumCount, bpc.getMaximumCount());
                assertEquals("Minimum mem", minimumMem, bpc.getMinimumMemory());
                assertEquals("Maximum mem", maximumMem, bpc.getMaximumMemory());
                assertEquals("Reserved mem", reservedMem, bpc.getReservedMemory());
                assertEquals("Fraction", 0, Float.compare(fraction, bpc.getFraction()));
            } else {
                assertEquals("Maximum count", 0, bpc.getMaximumCount());
            }
        }
    }

    @Test
    public void testLoadPropertiesBufferSpecifications() throws Exception {
        final Properties properties = new Properties();
        BufferPoolConfiguration bpc;
        properties.put("buffer.count.1024", "500");
        bpc = testLoadPropertiesBufferSpecificationsHelper(properties).getBufferPoolMap().get(1024);
        assertEquals(500, bpc.getMaximumCount());
        properties.put("buffer.memory.16384", "1M,1G,128M,0.6");
        bpc = testLoadPropertiesBufferSpecificationsHelper(properties).getBufferPoolMap().get(16384);
        assertEquals(16384, bpc.getBufferSize());
        assertEquals(MEGA, bpc.getMinimumMemory());
        assertEquals(GIGA, bpc.getMaximumMemory());
        assertEquals(128 * MEGA, bpc.getReservedMemory());
        assertEquals(0, Float.compare(0.6f, bpc.getFraction()));
        properties.put("buffer.memory.1024", "1M");
        try {
            testLoadPropertiesBufferSpecificationsHelper(properties).getBufferPoolMap().get(1024).getMaximumCount();
            fail("Exception not thrown");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testBrokenServerConfiguration() throws Exception {
        BufferPoolConfiguration bpc = new Configuration().getBufferPoolMap().get(16384);
        bpc.parseBufferMemory(16384, "buffer.memory.16384", "20M,512G,64M,0.50");
        final int bufferCount = bpc.computeBufferCount(7944576L * 1024L);
    }

    @Test
    public void testLoadFromPropertiesResource() throws Exception {
        Configuration configuration = new Configuration();
        configuration.readPropertiesFile(RESOURCE_NAME);
        assertEquals("Should have read properties from resource", 32, configuration.getBufferPoolMap().get(16384)
                .getMinimumCount());
        assertTrue("Should contain expected text 'hwdemo'", configuration.getJournalPath().contains("hwdemo"));
    }

    @Test
    public void canReinitialize() throws Exception {
        _persistit.close();
        assertTrue("Should have closed all volumes", _persistit.getVolumes().isEmpty());
        _persistit.initialize(_persistit.getConfiguration());
        assertTrue("Should have reopened volumes", !_persistit.getVolumes().isEmpty());
    }

    private Configuration testLoadPropertiesBufferSpecificationsHelper(final Properties properties) throws Exception {
        Configuration configuration = new Configuration();
        configuration.merge(properties);
        configuration.loadPropertiesBufferSpecifications();
        return configuration;
    }
}
