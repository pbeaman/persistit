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

import com.persistit.exception.PersistitException;

public class TemporaryVolume extends Volume {

    
    private TemporaryVolume(final Persistit persistit, final String path, final String name, final long id,
            final int bufferSize, long initialPages, long extensionPages, long maximumPages) throws PersistitException {
        super(persistit, path, name, id, bufferSize, initialPages, extensionPages, maximumPages);
        setTemporary();
    }
    
    @Override
    public boolean isTemporary() {
        return true;
    }
    
    public boolean clean() {
        if (getPool().invalidate(this)) {
            return false;
        }
        
        return true;
    }
}
