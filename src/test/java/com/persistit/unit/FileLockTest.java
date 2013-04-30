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

import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

import com.persistit.Persistit;
import com.persistit.PersistitUnitTestCase;
import com.persistit.exception.PersistitException;

/**
 * Test simple scenario to ensure a second Persistit instance referring to the
 * same volumes can't start.
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
            p2.setProperties(properties);
            p2.initialize();
            fail("Created second Persistit instance");
        } catch (final PersistitException pe) {
            // success - we intended to fail
        } finally {
            p2.close(false);
        }
        //
        _persistit.close(false);
        final Persistit p3 = new Persistit();
        // now this should succeed.
        try {
            p3.setProperties(properties);
            p3.initialize();
        } finally {
            p3.close(false);
        }
    }

    @Override
    public void runAllTests() throws Exception {
        testOverlap();
    }

}
