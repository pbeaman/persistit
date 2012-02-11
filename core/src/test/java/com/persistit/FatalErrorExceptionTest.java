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

package com.persistit;

import org.junit.Test;

import com.persistit.Persistit.FatalErrorException;
import com.persistit.unit.PersistitUnitTestCase;

public class FatalErrorExceptionTest extends PersistitUnitTestCase {
    

    @Test
    public void testFatalError() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "FatalErrorExceptionTest", true);
        ex.getValue().put(RED_FOX);
        ex.to(1).store();
        ex.fetch();
        _persistit.fatal("Test", new NullPointerException("Test"));
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
    }

}
