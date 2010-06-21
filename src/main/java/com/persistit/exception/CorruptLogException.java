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
public class CorruptLogException extends PersistitIOException {
    private static final long serialVersionUID = -5397911019132612370L;

    public CorruptLogException(String msg) {
        super(msg);
    }
}
