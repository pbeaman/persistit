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
 */

package com.persistit.test;

import com.persistit.IntegrityCheck;
import com.persistit.Volume;

public class IntegrityTest extends TestRunner.Test {
    String[] _args;
    Volume[] _volumes;
    IntegrityCheck[] _ichecks;
    int _icheckIndex = -1;

    private final static String SHORT_DESCRIPTION =
        "Perform volume integrity check";

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
    public void setupTest(String[] args) {

        if (args.length == 0) {
            args = new String[] { "persistit" };
        }

        _args = args;
        _volumes = new Volume[args.length];

        for (int index = 0; index < args.length; index++) {
            final Volume volume = getPersistit().getVolume(args[index]);
            if (volume == null) {
                println("Volume name not found: " + args[index]);
            } else {
                _volumes[index] = volume;
            }
        }
    }

    @Override
    protected void tearDownTest() {
        _volumes = null;

    }

    @Override
    public void runTest() {
        _ichecks = new IntegrityCheck[_volumes.length];
        final TestRunner.Result[] results =
            new TestRunner.Result[_volumes.length];
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

                    _result =
                        new TestRunner.Result(!icheck.hasFaults(), icheck
                            .toString());

                    results[_icheckIndex] = _result;

                    if (icheck.hasFaults()) {
                        passed = false;
                    }
                    resultCount++;
                } else {
                    _result =
                        new TestRunner.Result(false, "Volume name "
                            + _args[_icheckIndex] + " not found");
                    results[_icheckIndex] = _result;
                    resultCount++;
                }
            }
            if (resultCount > 1) {
                _result = new TestRunner.Result(passed, results);
            }

        } catch (final Exception ex) {
            _result = new TestRunner.Result(false, ex);
            println(ex.toString());
        }
        println();
        print("done");
    }

    public static void main(final String[] args) throws Exception {
        final IntegrityTest test = new IntegrityTest();
        test.runStandalone(args);
    }
}
