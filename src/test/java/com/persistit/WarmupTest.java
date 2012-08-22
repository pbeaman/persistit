/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

public class WarmupTest extends PersistitUnitTestCase {

    @Test
    public void testWarmup() throws Exception {
        Exchange ex = _persistit.getExchange("persistit", "WarmupTest", true);
        for (int i = 1; i <= 1000; i++) {
            ex.getValue().put(RED_FOX);
            ex.clear().append(i).store();
        }

        // Assumption: only one buffer pool is created
        int poolCount = 0;
        String pathName = "";
        final Buffer[] buff = new Buffer[100];
        for (final BufferPool p : _persistit.getBufferPoolHashMap().values()) {
            poolCount = p.getBufferCount();
            pathName = p.toString();
            for (int i = 0; i < poolCount; ++i) {
                buff[i] = p.getBufferCopy(i);
            }
        }

        final Properties properties = _persistit.getProperties();
        ex = null;
        _persistit.close();

        _persistit = new Persistit();
        _persistit.initialize(properties);

        int poolCount1 = 0;
        for (final BufferPool p : _persistit.getBufferPoolHashMap().values()) {
            poolCount1 = p.getBufferCount();
            for (int i = 0; i < poolCount1; ++i) {
                final Buffer bufferCopy = p.getBufferCopy(i);
                assertEquals(bufferCopy.getPageAddress(), buff[i].getPageAddress());
                assertEquals(bufferCopy.getPageType(), buff[i].getPageType());
                assertEquals(bufferCopy.getBufferSize(), buff[i].getBufferSize());
            }
        }
        assertEquals(poolCount, poolCount1);
    }
}
