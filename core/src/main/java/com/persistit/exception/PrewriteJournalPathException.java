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
 * Thrown when the Persistit is unable to open the prewrite journal file
 * specified by Persistit configration properties. See the
 * {@link com.persistit.Persistit#initialize(java.util.Properties) initialize}
 * method for further information.
 * 
 * @version 1.0
 */
public class PrewriteJournalPathException extends PersistitException {
    private static final long serialVersionUID = 7542536852904653252L;

    public PrewriteJournalPathException() {
        super();
    }

    public PrewriteJournalPathException(String msg) {
        super(msg);
    }
}