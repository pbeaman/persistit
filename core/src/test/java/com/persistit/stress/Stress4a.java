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

package com.persistit.stress;

import com.persistit.exception.PersistitException;

public class Stress4a extends Stress4Base {
    private final static String SHORT_DESCRIPTION = "Various sequential and random key/value pair manipulation";

    private final static String LONG_DESCRIPTION = "   Tests fidelity of read/read/traverse over short and long \r\n"
            + "   records, length up to 80K bytes";

    @Override
    public String shortDescription() {
        return SHORT_DESCRIPTION;
    }

    @Override
    public String longDescription() {
        return LONG_DESCRIPTION;
    }

    @Override
    public void repeatedTasks() throws PersistitException {
        describeTest("sequential write " + _total + " records 30 bytes long ");
        writeRecords(_total, false, 30, 30);
        verboseln();

        describeTest("sequential read " + _total
                + " short records 30 bytes long");
        readRecords(_total, false, 30, 30);
        verboseln();

        describeTest("traverse records forward");
        testForward();
        verboseln();

        describeTest("traverse records reverse");
        testReverse();
        verboseln();

        describeTest("read records");
        testReads(_total);
        verboseln();

        seed(_seed);
        describeTest("random write " + _total + " records 20-80 bytes long");
        writeRecords(_total, true, 20, 80);
        verboseln();

        seed(_seed);
        describeTest("random read " + _total + " records 20-80 bytes long");
        readRecords(_total, true, 20, 80);
        verboseln();

        seed(_seed * 7);
        describeTest("random write " + (_total / 50)
                + " records 20-8000 bytes long");
        writeRecords(_total / 50, true, 20, 8000);
        verboseln();

        seed(_seed * 7);
        describeTest("random read " + (_total / 50)
                + " records 20-8000 bytes long");
        readRecords(_total / 50, true, 20, 8000);
        verboseln();

        describeTest("traverse records in reverse order");
        testReverse();
        verboseln();

        seed(_seed * 7);
        describeTest("random write " + (_total / 200)
                + " records 20-80000 bytes long");
        writeRecords(_total / 200, true, 20, 80000);
        verboseln();

        seed(_seed * 7);
        describeTest("random read " + (_total / 200)
                + " records 20-80000 bytes long");
        readRecords(_total / 200, true, 20, 80000);
        verboseln();

        describeTest("traverse records in forward order");
        testReverse();
        verboseln();

        seed(_seed * 17);
        describeTest("randomly remove " + (_total / 3) + " records");
        removeRecords(_seed, true);
        verboseln();

        describeTest("traverse records in reverse order");
        testForward();
        verboseln();
    }

    public static void main(final String[] args) {
        new Stress4a().runStandalone(args);
    }
}
