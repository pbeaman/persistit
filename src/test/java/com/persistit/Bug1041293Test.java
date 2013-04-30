/**
 * Copyright 2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit;

import org.junit.Test;

import com.persistit.exception.BufferSizeUnavailableException;
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

    @Test(expected = BufferSizeUnavailableException.class)
    public void mismatchedVolumeSpecificationNPE() throws Exception {
        final Configuration config = _persistit.getConfiguration();
        final VolumeSpecification vspec = config.volumeSpecification("${datapath}/test,pageSize:2048,create,"
                + "initialPages:100,extensionPages:100,maximumPages:25000");
        final Volume volume = _persistit.loadVolume(vspec);
        volume.open(_persistit);
    }

    @Test(expected = BufferSizeUnavailableException.class)
    public void mismatchedTemporaryVolumePageSize() throws Exception {
        _persistit.createTemporaryVolume(2048);
    }

}
