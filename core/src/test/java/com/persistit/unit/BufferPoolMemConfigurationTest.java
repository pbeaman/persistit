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
