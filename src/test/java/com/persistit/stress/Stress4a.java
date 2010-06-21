/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * Created on Feb 24, 2004
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
        println();

        describeTest("sequential read " + _total
                + " short records 30 bytes long");
        readRecords(_total, false, 30, 30);
        println();

        describeTest("traverse records forward");
        testForward();
        println();

        describeTest("traverse records reverse");
        testReverse();
        println();

        describeTest("read records");
        testReads(_total);
        println();

        seed(_seed);
        describeTest("random write " + _total + " records 20-80 bytes long");
        writeRecords(_total, true, 20, 80);
        println();

        seed(_seed);
        describeTest("random read " + _total + " records 20-80 bytes long");
        readRecords(_total, true, 20, 80);
        println();

        seed(_seed * 7);
        describeTest("random write " + (_total / 50)
                + " records 20-8000 bytes long");
        writeRecords(_total / 50, true, 20, 8000);
        println();

        seed(_seed * 7);
        describeTest("random read " + (_total / 50)
                + " records 20-8000 bytes long");
        readRecords(_total / 50, true, 20, 8000);
        println();

        describeTest("traverse records in reverse order");
        testReverse();
        println();

        seed(_seed * 7);
        describeTest("random write " + (_total / 200)
                + " records 20-80000 bytes long");
        writeRecords(_total / 200, true, 20, 80000);
        println();

        seed(_seed * 7);
        describeTest("random read " + (_total / 200)
                + " records 20-80000 bytes long");
        readRecords(_total / 200, true, 20, 80000);
        println();

        describeTest("traverse records in forward order");
        testReverse();
        println();

        seed(_seed * 17);
        describeTest("randomly remove " + (_total / 3) + " records");
        removeRecords(_seed, true);
        println();

        describeTest("traverse records in reverse order");
        testForward();
        println();
    }

    private void describeTest(final String m) {
        print(m);
        print(": ");
        for (int i = m.length(); i < 52; i++) {
            print(" ");
        }
    }

    public static void main(final String[] args) throws Exception {
        new Stress4a().runStandalone(args);
    }
}
