/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

import com.persistit.Management.BufferPoolInfo;

public class BufferPoolMemConfigurationTest extends PersistitUnitTestCase {

    @Override
    public void setUp() {
        // Persistit setup is done within the test itself
    }

    @Test
    public void testBufferMemConfiguration() throws Exception {
        final Properties properties = UnitTestProperties.getPropertiesByMemory(true, "1M");
        _persistit.initialize(properties);
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
            _persistit.initialize(properties);
            fail("Accepted allocation 10000 bytes");
        } catch (final IllegalArgumentException e) {
            // okay
        }

        properties = UnitTestProperties.getPropertiesByMemory(true, "10000G");
        try {
            _persistit.initialize(properties);
            fail("Accepted allocation 10000G bytes");
        } catch (final IllegalArgumentException e) {
            // okay
        }

        properties = UnitTestProperties.getPropertiesByMemory(true, "10M");
        properties.put("buffer.count.16384", "1234");
        try {
            _persistit.initialize(properties);
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
