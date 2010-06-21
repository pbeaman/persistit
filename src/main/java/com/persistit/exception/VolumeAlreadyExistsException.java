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
 * Thrown when the Persistit configuration specifies the <tt>createOnly</tt>
 * flag for a volume that already exists, or when a the configuration specifies
 * a page size for a volume that does not the page size of an existing volume.
 * 
 * @version 1.0
 */
public class VolumeAlreadyExistsException extends PersistitException {
    private static final long serialVersionUID = -5943311415307074464L;

    public VolumeAlreadyExistsException() {
        super();
    }

    public VolumeAlreadyExistsException(String msg) {
        super(msg);
    }
}
