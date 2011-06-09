/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit.unit;

import java.util.Properties;

import org.junit.Test;

import com.persistit.Persistit;
import com.persistit.exception.PersistitException;

/**
 * Test simple scenario to ensure a second Persistit instance referring to
 * the same volumes can't start.
 * 
 * @author peter
 *
 */
public class FileLockTest extends PersistitUnitTestCase {

    @Test
    public void testOverlap() throws Exception {
        final Properties properties = _persistit.getProperties();
        final Persistit p2 = new Persistit();
        try {
            p2.initialize(properties);
            fail("Created second Persistit instance");
        } catch (PersistitException pe) {
            // success - we intended to fail
        } finally {
            p2.close(false);
        }
        //
        _persistit.close(false);
        // now this should succeed.
        try {
        p2.initialize(properties);
        } finally {
            p2.close(false);
        }
    }
    
    
    
    @Override
    public void runAllTests() throws Exception {
        testOverlap();
    }
    
}
