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

package com.persistit;

import static org.junit.Assert.fail;

import org.junit.Test;

import com.persistit.Persistit.FatalErrorException;

public class FatalErrorExceptionTest extends PersistitUnitTestCase {

    @Test
    public void testFatalError() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "FatalErrorExceptionTest", true);
        ex.getValue().put(RED_FOX);
        ex.to(1).store();
        ex.fetch();
        try {
            _persistit.fatal("Test", new NullPointerException("Test"));
        } catch (FatalErrorException e) {
            // expected
        }
        try {
            ex.store();
            fail("Expected FatalErrorException");
        } catch (FatalErrorException e) {
            // expected
        }
        try {
            ex.fetch();
            fail("Expected FatalErrorException");
        } catch (FatalErrorException e) {
            // expected
        }
        _persistit.crash();
    }

}
