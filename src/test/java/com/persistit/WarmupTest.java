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

import static org.junit.Assert.*;
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
        Buffer[] buff = new Buffer[100];
        for (BufferPool p: _persistit.getBufferPoolHashMap().values()) {
        	poolCount = p.getBufferCount();
                pathName = p.toString();
        	for (int i = 0; i < poolCount; ++i) {
        		buff[i] = p.getBufferCopy(i);
        	}
        }
        
        Properties properties = _persistit.getProperties();
        ex = null;
        _persistit.close();

        _persistit = new Persistit();
        _persistit.initialize(properties);
        
        int poolCount1 = 0;
        for (BufferPool p: _persistit.getBufferPoolHashMap().values()) {
        	poolCount1 = p.getBufferCount();
        	for (int i = 0; i < poolCount1; ++i) {
        		Buffer bufferCopy = p.getBufferCopy(i);
        		assertEquals(bufferCopy.getPageAddress(), buff[i].getPageAddress());
        		assertEquals(bufferCopy.getPageType(), buff[i].getPageType());
        		assertEquals(bufferCopy.getBufferSize(), buff[i].getBufferSize());
        	}
        }
        assertEquals(poolCount, poolCount1);         
    }
}
