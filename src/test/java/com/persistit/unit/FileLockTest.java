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

import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

import com.persistit.Persistit;
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
            p2.initialize(properties);
            fail("Created second Persistit instance");
        } catch (PersistitException pe) {
            // success - we intended to fail
        } finally {
            p2.close(false);
        }
        //
        _persistit.close(false);
        final Persistit p3 = new Persistit();
        // now this should succeed.
        try {
            p3.initialize(properties);
        } finally {
            p3.close(false);
        }
    }

    @Override
    public void runAllTests() throws Exception {
        testOverlap();
    }

}
