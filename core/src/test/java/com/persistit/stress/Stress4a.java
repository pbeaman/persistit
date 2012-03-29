/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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

        describeTest("sequential read " + _total + " short records 30 bytes long");
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
        describeTest("random write " + (_total / 50) + " records 20-8000 bytes long");
        writeRecords(_total / 50, true, 20, 8000);
        verboseln();

        seed(_seed * 7);
        describeTest("random read " + (_total / 50) + " records 20-8000 bytes long");
        readRecords(_total / 50, true, 20, 8000);
        verboseln();

        describeTest("traverse records in reverse order");
        testReverse();
        verboseln();

        seed(_seed * 7);
        describeTest("random write " + (_total / 200) + " records 20-80000 bytes long");
        writeRecords(_total / 200, true, 20, 80000);
        verboseln();

        seed(_seed * 7);
        describeTest("random read " + (_total / 200) + " records 20-80000 bytes long");
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
