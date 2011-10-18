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

import org.junit.Test;

import com.persistit.exception.PersistitException;

public class JDK14LoggerTest extends PersistitUnitTestCase {

    @Test
    public void test1() throws PersistitException {
        System.out.print("test1 ");
        System.out.println("- done");
    }

    public static void main(final String[] args) throws Exception {
        new JDK14LoggerTest().initAndRunTest();
    }

    @Override
    public void runAllTests() throws Exception {
        test1();
    }

}
