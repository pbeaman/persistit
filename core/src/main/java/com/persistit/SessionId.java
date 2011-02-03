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

import java.util.concurrent.atomic.AtomicInteger;

public class SessionId {

    final static AtomicInteger counter = new AtomicInteger(1);
    
    private final int _id = counter.getAndIncrement();
    
    @Override
    public boolean equals(final Object id) {
        return this._id == ((SessionId)id)._id;
    }
    
    @Override
    public int hashCode() {
        return _id;
    }
    
    @Override
    public String toString() {
        return "[" + _id + "]";
    }
}
