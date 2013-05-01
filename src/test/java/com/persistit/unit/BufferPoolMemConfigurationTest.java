/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

import com.persistit.Management.BufferPoolInfo;
import com.persistit.PersistitUnitTestCase;

public class BufferPoolMemConfigurationTest extends PersistitUnitTestCase {

    @Override
    public void setUp() {
        // Persistit setup is done within the test itself
    }

    @Test
    public void testBufferMemConfiguration() throws Exception {
        final Properties properties = UnitTestProperties.getPropertiesByMemory(true, "1M");
        _persistit.setProperties(properties);
        _persistit.initialize();
        final BufferPoolInfo[] infoArray = _persistit.getManagement().getBufferPoolInfoArray();
        assertEquals(1, infoArray.length);
        final int bufferSize = infoArray[0].getBufferSize();
        final int bufferCount = infoArray[0].getBufferCount();
        assertTrue(bufferSize * bufferCount < 1024 * 1024);
        assertTrue(bufferSize * bufferCount > 512 * 1024);
        _persistit.close();
    }

    @Test
    public void testBufferMemConfigurationErrors() throws Exception {
        Properties properties = UnitTestProperties.getPropertiesByMemory(true, "1000,999");
        try {
            _persistit.setProperties(properties);
            _persistit.initialize();
            fail("Accepted allocation 10000 bytes");
        } catch (final IllegalArgumentException e) {
            // okay
        }

        properties = UnitTestProperties.getPropertiesByMemory(true, "10000G");
        try {
            _persistit.setProperties(properties);
            _persistit.initialize();
            fail("Accepted allocation 10000G bytes");
        } catch (final IllegalArgumentException e) {
            // okay
        }

        properties = UnitTestProperties.getPropertiesByMemory(true, "10M");
        properties.put("buffer.count.16384", "1234");
        try {
            _persistit.setProperties(properties);
            _persistit.initialize();
            fail("Accepted allocation specified by both count and size");
        } catch (final IllegalArgumentException e) {
            // okay
        }
        _persistit.close();

    }

    @Override
    public void runAllTests() throws Exception {
        // TODO Auto-generated method stub

    }

}
