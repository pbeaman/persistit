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

package com.persistit;

import com.persistit.test.AbstractTestRunnerItem;
import com.persistit.test.TestResult;

public class ConfirmEmptyVolume extends AbstractTestRunnerItem {

    Volume[] _volumes;

    private final static String SHORT_DESCRIPTION = "Confirm volume is empty";

    private final static String LONG_DESCRIPTION = SHORT_DESCRIPTION;

    @Override
    public String shortDescription() {
        return SHORT_DESCRIPTION;
    }

    @Override
    public String longDescription() {
        return LONG_DESCRIPTION;
    }

    @Override
    public double getProgress() {
        return 0;
    }

    @Override
    public String getProgressString() {
        return "unknown";
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (_args.length == 0) {
            _args = new String[] { "persistit" };
        }

        _volumes = new Volume[_args.length];

        for (int index = 0; index < _args.length; index++) {
            final Volume volume = getPersistit().getVolume(_args[index]);
            if (volume == null) {
                println("Volume name not found: " + _args[index]);
            } else {
                _volumes[index] = volume;
            }
        }
    }

    @Override
    public void tearDown() throws Exception {
        _volumes = null;
        super.tearDown();
    }

    public void confirmEmpty() {
        final TestResult[] results = new TestResult[_volumes.length];
        int resultCount = 0;
        boolean passed = true;
        try {
            for (int index = 0; index < _volumes.length; index++) {
                final Volume volume = _volumes[index];
                if (volume != null) {
                    final String[] treeNames = volume.getTreeNames();
                    final boolean empty = treeNames.length == 0;
                    if (empty) {
                        results[resultCount] = new TestResult(true, "no trees");
                    } else {
                        final StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < treeNames.length; i++) {
                            sb.append(treeNames[i]);
                            sb.append(" ");
                        }
                        results[resultCount] = new TestResult(false, sb.toString());
                        passed = false;
                    }
                    resultCount++;
                } else {
                    _result = new TestResult(false, "Volume name " + _args[index] + " not found");
                    results[index] = _result;
                    resultCount++;
                }
            }
            if (resultCount > 1) {
                _result = new TestResult(passed, results);
            }

        } catch (final Exception ex) {
            _result = new TestResult(false, ex);
            println(ex.toString());
        }
        println();
        print("done");
    }

    @Override
    public void executeTest() throws Exception {
        confirmEmpty();
    }

    public static void main(final String[] args) {
        final ConfirmEmptyVolume test = new ConfirmEmptyVolume();
        test.runStandalone(args);
    }
}
