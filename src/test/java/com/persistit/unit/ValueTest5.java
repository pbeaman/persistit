/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
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

import static org.junit.Assert.assertEquals;

import java.io.Serializable;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.PersistitUnitTestCase;
import com.persistit.encoding.CoderManager;
import com.persistit.exception.PersistitException;

public class ValueTest5 extends PersistitUnitTestCase {
    /**
     * Tests JSA 1.1 default serialization. Requires the
     * enableCompatibleConstructors to be true.
     */

    Exchange _exchange;

    public enum Job {
        COOK, BOTTLEWASHER, PROGRAMMER, JANITOR, SALEMAN,
    };

    public static class S implements Serializable {
        private final static long serialVersionUID = 1;
        Job _myJob;
        Job _yourJob;

        S(final Job m, final Job y) {
            _myJob = m;
            _yourJob = y;
        }

        @Override
        public String toString() {
            return _myJob + ":" + _yourJob;
        }

    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _exchange = _persistit.getExchange("persistit", getClass().getSimpleName(), true);
    }

    @Override
    public void tearDown() throws Exception {
        _persistit.releaseExchange(_exchange);
        _exchange = null;
        super.tearDown();
    }

    @Test
    public void test1() throws PersistitException {
        System.out.print("test1 ");
        final S s = new S(Job.COOK, Job.BOTTLEWASHER);
        _exchange.getValue().put(s);
        _exchange.clear().append("test1").store();
        final Object x = _exchange.getValue().get();
        assertEquals("COOK:BOTTLEWASHER", x.toString());

        System.out.println("- done");
    }

    public static void main(final String[] args) throws Exception {
        new ValueTest5().initAndRunTest();
    }

    @Override
    public void runAllTests() throws Exception {
        _exchange = _persistit.getExchange("persistit", "ValueTest5", true);
        final CoderManager cm = _persistit.getCoderManager();

        test1();

        _persistit.setCoderManager(cm);
    }

}
