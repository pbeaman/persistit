package com.persistit;
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

public class ReentrantResourceHolder {

    private final SharedResource _resource;
    private int _claimCount;
    
    ReentrantResourceHolder(final SharedResource r) {
        _resource = r;
    }
    
    void verifyReleased() {
        Debug.debug1(_claimCount != 0);
        _claimCount = 0;
    }
    
    boolean claim(final boolean writer) {
        return claim(writer, SharedResource.DEFAULT_MAX_WAIT_TIME);
    }

    boolean claim(final boolean writer, final long timeout) {
        if (_claimCount == 0) {
            if (!_resource.claim(writer)) {
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
