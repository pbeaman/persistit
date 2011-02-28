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

import java.io.IOException;

/**
 * Thrown when attempting to create new {@link com.persistit.Tree} if another
 * <code>Tree</code> having the same name already exists.
 * 
 * @version 1.0
 */
public class TreeAlreadyExistsException extends IOException {
    private static final long serialVersionUID = 4038415783689624531L;

    public TreeAlreadyExistsException() {
        super();
    }

    public TreeAlreadyExistsException(String msg) {
        super(msg);
    }
}
