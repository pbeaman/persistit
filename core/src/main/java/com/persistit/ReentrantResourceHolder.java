/**
 * Copyright (C) 2011 Akiban Technologies Inc. This program is free software:
 * you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License, version 3, as published by the Free Software
 * Foundation.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see http://www.gnu.org/licenses.
 */
package com.persistit;

import com.persistit.exception.PersistitInterruptedException;
import com.persistit.util.Debug;

/**
 * A container for a SharedResource that experiences reentrant claims (locks).
 * {@link Exchange} uses this to hold a Tree object, which experiences this pattern.
 * The purpose is to keep a count of so that when an Exchange has finished using
 * the Tree, it can verify that all claims were released. This is primarily for
 * debugging; correct code will always release each claim.
 */

class ReentrantResourceHolder {

    private final SharedResource _resource;
    private int _claimCount;

    ReentrantResourceHolder(final SharedResource r) {
        _resource = r;
    }

    void verifyReleased() {
        Debug.$assert0.t(_claimCount == 0);
        _claimCount = 0;
    }

    boolean claim(final boolean writer) throws PersistitInterruptedException {
        return claim(writer, SharedResource.DEFAULT_MAX_WAIT_TIME);
    }

    boolean claim(final boolean writer, final long timeout) throws PersistitInterruptedException {
        if (_claimCount == 0) {
            if (!_resource.claim(writer, timeout)) {
                return false;
            } else {
                _claimCount++;
                return true;
            }
        } else {
            if (writer && !_resource.isWriter()) {
                if (!_resource.upgradeClaim()) {
                    return false;
                }
            }
            _claimCount++;
            return true;
        }
    }

    void release() {
        if (_claimCount <= 0) {
            throw new IllegalStateException("This thread holds no claims");
        }
        if (--_claimCount == 0) {
            _resource.release();
        }
    }

    boolean upgradeClaim() {
        return _resource.upgradeClaim();
    }
}
