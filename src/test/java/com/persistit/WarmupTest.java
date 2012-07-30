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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

import com.persistit.BufferPool.BufferHolder;
import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.*;
import org.junit.After;
import org.junit.Before;

public class WarmupTest extends PersistitUnitTestCase {
    
    public void setUp(String warmup) throws Exception {
        start = System.currentTimeMillis();
        checkNoPersistitThreads();
        Properties p = getProperties(true);
        p.setProperty("bufferwarmupenabled", warmup);
        _persistit.initialize(p);
    }

    @Test
    public void testWarmup() throws PersistitException, Exception {
        System.out.println("Test warmup");
        warmup("true");
    }
    
    @Test
    public void testNoWarmup() throws PersistitException, Exception {
        System.out.println("Test no warmup");
        warmup("false");
    }
    
    private void warmup(String warmup) throws PersistitException, Exception {
        int count = 3;

        addBuffers();
        tearDown();
        
        for (int i = 0; i < count; ++i ) {
            _persistit = new Persistit();
            setUp(warmup);
            addBuffers();
            tearDown();
        }
        _persistit = new Persistit();
    }
    
    private void addBuffers() throws PersistitException {
        final Volume vol = _persistit.createTemporaryVolume();
        final Exchange ex = _persistit.getExchange(vol, "WarmupTest", true);
        for (int i = 0; i < 100; ++i ) {
            ex.append(i).store();
        }
    }
}
