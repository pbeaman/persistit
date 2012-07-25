/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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
