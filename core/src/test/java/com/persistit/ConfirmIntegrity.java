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

public class ConfirmIntegrity extends AbstractTestRunnerItem {

    Volume[] _volumes;
    IntegrityCheck[] _ichecks;
    int _icheckIndex = -1;

    private final static String SHORT_DESCRIPTION = "Perform volume integrity check";

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
        if ((_ichecks == null) || (_icheckIndex < 0)) {
            return 0;
        }
        if (_icheckIndex == _ichecks.length) {
            return 1.0;
        }
        final IntegrityCheck icheck = _ichecks[_icheckIndex];
        if (icheck != null) {
            return icheck.getProgress();
        }
        return 0;
    }

    @Override
    public String getProgressString() {
        if ((_ichecks == null) || (_icheckIndex < 0)) {
            return "not started";
        }
        if (_icheckIndex == _ichecks.length) {
            return "done";
        }
        final IntegrityCheck icheck = _ichecks[_icheckIndex];
        if (icheck != null) {
            return icheck.getStatus();
        }
        return "unknown";
    }

    @Override
    public void setUp() throws Exception {
        super.setUp(false);
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
        _ichecks = null;
        super.tearDown();
    }

    public void checkIntegrity() {
        _ichecks = new IntegrityCheck[_volumes.length];
        final TestResult[] results = new TestResult[_volumes.length];
        int resultCount = 0;
        boolean passed = true;
        try {
            for (_icheckIndex = 0; _icheckIndex < _volumes.length; _icheckIndex++) {
                final Volume volume = _volumes[_icheckIndex];
                if (volume != null) {
                    print("Performing integrity check on " + volume);
                    final IntegrityCheck icheck = new IntegrityCheck(getPersistit());
                    _ichecks[_icheckIndex] = icheck;

                    icheck.checkVolume(volume);
                    println(" - " + icheck.toString(true));

                    _result = new TestResult(!icheck.hasFaults(), icheck.toString());

                    results[_icheckIndex] = _result;

                    if (icheck.hasFaults()) {
                        passed = false;
                    }
                    resultCount++;
                } else {
                    _result = new TestResult(false, "Volume name " + _args[_icheckIndex] + " not found");
                    results[_icheckIndex] = _result;
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
        println("done");
    }

    @Override
    public void executeTest() throws Exception {
        checkIntegrity();
    }

    public static void main(final String[] args) {
        final ConfirmIntegrity test = new ConfirmIntegrity();
        test.runStandalone(args);
    }
}
