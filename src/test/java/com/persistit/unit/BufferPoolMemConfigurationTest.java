/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

import com.persistit.PersistitUnitTestCase;
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
        BufferPoolInfo[] infoArray = _persistit.getManagement().getBufferPoolInfoArray();
        assertEquals(1, infoArray.length);
        int bufferSize = infoArray[0].getBufferSize();
        int bufferCount = infoArray[0].getBufferCount();
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
        } catch (IllegalArgumentException e) {
            // okay
        }

        properties = UnitTestProperties.getPropertiesByMemory(true, "10000G");
        try {
            _persistit.initialize(properties);
            fail("Accepted allocation 10000G bytes");
        } catch (IllegalArgumentException e) {
            // okay
        }

        properties = UnitTestProperties.getPropertiesByMemory(true, "10M");
        properties.put("buffer.count.16384", "1234");
        try {
            _persistit.initialize(properties);
            fail("Accepted allocation specified by both count and size");
        } catch (IllegalArgumentException e) {
            // okay
        }
        _persistit.close();

    }

    @Override
    public void runAllTests() throws Exception {
        // TODO Auto-generated method stub

    }

}
