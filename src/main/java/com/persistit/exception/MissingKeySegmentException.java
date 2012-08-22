/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit.exception;

/**
 * Thrown by decoding methods of the {@link com.persistit.Key} class when there
 * is no remaining key segment to decode.
 * 
 * @version 1.0
 */
public class MissingKeySegmentException extends RuntimeException {
    private static final long serialVersionUID = 7423673980055887541L;

    public MissingKeySegmentException() {
        super();
    }

    public MissingKeySegmentException(final String msg) {
        super(msg);
    }
}
