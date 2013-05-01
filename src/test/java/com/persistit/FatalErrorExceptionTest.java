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
        } catch (final FatalErrorException e) {
            // expected
        }
        try {
            ex.store();
            fail("Expected FatalErrorException");
        } catch (final FatalErrorException e) {
            // expected
        }
        try {
            ex.fetch();
            fail("Expected FatalErrorException");
        } catch (final FatalErrorException e) {
            // expected
        }
        _persistit.crash();
    }

}
