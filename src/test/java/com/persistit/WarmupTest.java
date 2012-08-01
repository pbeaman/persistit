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
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

class BuffInfo {
	public long page;
	public Volume vol;
}

public class WarmupTest extends PersistitUnitTestCase {
	
	private final String DEFAULT_LOG_PATH = "/tmp/persistit_test_data/buffer_pool.log";

	@Test
    public void testWarmup() throws Exception {
        Exchange ex = _persistit.getExchange("persistit", "WarmupTest", true);
        for (int i = 1; i <= 1000; i++) {
            ex.getValue().put(RED_FOX);
            ex.clear().append(i).store();
        }
        
        // Assumption: only one buffer pool is created
        int poolCount = 0;
        Buffer[] buff = new Buffer[100];
        for (BufferPool p: _persistit.getBufferPoolHashMap().values()) {
        	poolCount = p.getBufferCount();
        	for (int i = 0; i < poolCount; ++i) {
        		buff[i] = p.getBufferCopy(i);
        	}
        }
        
        BuffInfo[] buffInfo = new BuffInfo[100];
        File file = new File(DEFAULT_LOG_PATH);
        if (file.exists()) {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String currLine;
            int count = 0;
            while ((currLine = reader.readLine()) != null) {
                String[] info = currLine.split(" ");
                buffInfo[count].page = Long.parseLong(info[0]);
                buffInfo[count].vol = _persistit.getVolume(info[1]); 
                count++;
            }
            reader.close();
        }
        
        Properties properties = _persistit.getProperties();
        properties.setProperty("bufferwarmupenabled", "true");
        ex = null;
        _persistit.close();

        _persistit = new Persistit();
        _persistit.initialize(properties);
        
        int poolCount1 = 0;
        for (BufferPool p: _persistit.getBufferPoolHashMap().values()) {
        	poolCount1 = p.getBufferCount();
        	for (int i = 0; i < poolCount1; ++i) {
        		Buffer bufferCopy = p.getBufferCopy(i);
        		Buffer bufferFileCopy;
        		if (buffInfo[i] != null) {
        			bufferFileCopy = p.getBufferCopy(buffInfo[i].vol, buffInfo[i].page);
        			assertEquals(bufferFileCopy.getPageAddress(), bufferCopy.getPageAddress());
        			assertEquals(bufferFileCopy.getPageType(), bufferCopy.getPageType());
        		}
        		assertEquals(bufferCopy.getPageAddress(), buff[i].getPageAddress());
        		assertEquals(bufferCopy.getPageType(), buff[i].getPageType());
        		assertEquals(bufferCopy.getBufferSize(), buff[i].getBufferSize());
        	}
        }
        assertEquals(poolCount, poolCount1);
           
    }
}
