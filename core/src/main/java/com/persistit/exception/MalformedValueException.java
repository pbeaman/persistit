/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Jun 9, 2004
 */
package com.persistit.exception;

/**
 * Thrown by decoding methods of the {@link com.persistit.Value} class when the
 * serialized byte array is corrupt. This is a catastrophic failure that
 * signifies external volume file corruption.
 * 
 * @version 1.0
 */
public class MalformedValueException extends RuntimeException {
    private static final long serialVersionUID = 5868710861424952291L;

    public MalformedValueException() {
        super();
    }

    public MalformedValueException(String msg) {
        super(msg);
    }
}
