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
 * Created on Mar 8, 2005
 */
package com.persistit.ui;
import javax.swing.AbstractListModel;
/**
 * A ListModel used to populate lists of volumes and trees.
 * @author Peter Beaman
 * @version 1.0
 */
public class ManagementListModel
extends AbstractListModel
{
    private Object[] _infoArray;

    public void setInfoArray(Object[] array)
    {
        int oldLength = _infoArray == null ? 0 : _infoArray.length;
        int newLength = array == null ? 0 : array.length;
        if (oldLength != newLength && oldLength != 0)
        {
            fireIntervalRemoved(this, 0, oldLength);
        }
        Object[] oldArray = _infoArray;
        _infoArray = array;
        if (oldLength != newLength)
        {
            fireIntervalAdded(this, 0, newLength);
        }
        else if (newLength > 0)
        {
            int changed = -1;
            for (int index = 0; changed == -1 && index < newLength; index++)
            {
                if (!array[index].equals(oldArray[index]))
                {
                    changed = newLength;
                }
            }
            if (changed >= 0)
            {
                fireContentsChanged(this, changed, newLength);
            }
        }
    }
    
    public Object[] getInfoArray()
    {
        return _infoArray;
    }


    
    public Object getElementAt(int index)
    {
        if (_infoArray != null && index >= 0 && index < _infoArray.length)
        {
            return _infoArray[index];
        }
        return null;
    }
    
    public int getSize()
    {
        if (_infoArray == null) return 0;
        else return _infoArray.length;
    }
}
