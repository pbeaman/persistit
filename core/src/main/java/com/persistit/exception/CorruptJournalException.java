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

package com.persistit.exception;

/**
 * Thrown if the prewrite journal file is corrupt. Generally it will be
 * necessary to delete the prewrite journal file to resolve this. In so doing,
 * critical information needed to recover the state of one or more
 * {@link com.persistit.Volume}s may be lost.
 * 
 * @version 1.0
 */
public class CorruptJournalException extends PersistitIOException {
    private static final long serialVersionUID = -5397911019132612370L;

    public CorruptJournalException(String msg) {
        super(msg);
    }
}
