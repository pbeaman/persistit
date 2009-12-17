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

import com.persistit.Persistit;
import com.persistit.Volume;

public class ConfirmEmptyVolumeTest extends TestRunner.Test {
    String[] _args;
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
        final TestRunner.Result[] results =
            new TestRunner.Result[_volumes.length];
        int resultCount = 0;
        boolean passed = true;
        try {
            for (int index = 0; index < _volumes.length; index++) {
                final Volume volume = _volumes[index];
                if (volume != null) {
                    final String[] treeNames = volume.getTreeNames();
                    final boolean empty = treeNames.length == 0;
                    if (empty) {
                        results[resultCount] = new TestRunner.Result(true);
                    } else {
                        final StringBuffer sb = new StringBuffer();
                        for (int i = 0; i < treeNames.length; i++) {
                            sb.append(treeNames[i]);
                            sb.append(" ");
                        }
                        results[resultCount] =
                            new TestRunner.Result(false, sb.toString());
                        passed = false;
                    }
                    resultCount++;
                } else {
                    _result =
                        new TestRunner.Result(false, "Volume name "
                            + _args[index] + " not found");
                    results[index] = _result;
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
        final ConfirmEmptyVolumeTest test = new ConfirmEmptyVolumeTest();
        test.runStandalone(args);
    }
}
