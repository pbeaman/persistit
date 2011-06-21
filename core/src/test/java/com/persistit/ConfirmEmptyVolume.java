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
