/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit;

import org.junit.Test;

import com.persistit.exception.UnderSpecifiedVolumeException;

/**
 * https://bugs.launchpad.net/akiban-persistit/+bug/1041293
 * 
 * If a VolumeSpecification specifies a page size that doesn't correspond to any
 * of the available buffer pool sizes, a NullPointerException is thrown when the
 * volume is being loaded (created) .
 * 
 * Something a little more helpful should probably be done.
 */

public class Bug1041293Test extends PersistitUnitTestCase {

    @Test(expected = UnderSpecifiedVolumeException.class)
    public void underSpecifiedVolume() throws Exception {
        final Configuration config = _persistit.getConfiguration();
        final VolumeSpecification vspec = config.volumeSpecification("${datapath}/test");
        final Volume volume = _persistit.loadVolume(vspec);
        volume.open(_persistit);
    }

    @Test(expected = IllegalStateException.class)
    public void mismatchedVolumeSpecificationNPE() throws Exception {
        final Configuration config = _persistit.getConfiguration();
        final VolumeSpecification vspec = config.volumeSpecification("${datapath}/test,pageSize:2048,create,"
                + "initialPages:100,extensionPages:100,maximumPages:25000");
        final Volume volume = _persistit.loadVolume(vspec);
        volume.open(_persistit);
    }

    @Test(expected = IllegalStateException.class)
    public void mismatchedTemporaryVolumePageSize() throws Exception {
        _persistit.createTemporaryVolume(2048);
    }

}
