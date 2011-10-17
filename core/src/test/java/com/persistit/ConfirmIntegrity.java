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
        println();
        print("done");
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
