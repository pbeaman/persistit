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
 * Created on Feb 11, 2004
 */
package com.persistit.ui;

import java.util.ArrayList;
import java.util.List;

import javax.swing.AbstractListModel;
import javax.swing.ComboBoxModel;

import com.persistit.Management;
import com.persistit.Management.TreeInfo;
import com.persistit.Management.VolumeInfo;

class VTComboBoxModel
extends AbstractListModel
implements ComboBoxModel
{

    private VTComboBoxModel _parent;
    
    private List _cachedList = new ArrayList();
    private Object _selectedItem;
    private long _validTime;
    private Management _management;
    
    public VTComboBoxModel(VTComboBoxModel vtcmb, Management management)
    {
        _parent = vtcmb;
        _management = management;
    }
    
    public Object getElementAt(int index)
    {
        populateList();
        if (index >= 0 && index < _cachedList.size())
        {
            return _cachedList.get(index);
        }
        return null;
    }
    
    public int getSize()
    {
        populateList();
        return _cachedList.size();
    }
    
    public void setSelectedItem(Object item)
    {
        _selectedItem = item;
    }
    
    public Object getSelectedItem()
    {
        return _selectedItem;
    }
    
    private void populateList()
    {
        long now = System.currentTimeMillis();
        if (now - _validTime < 1000) return;
        _validTime = now;
        _cachedList.clear();
        try
        {
            if (_parent == null)
            {
                VolumeInfo[] volumes = 
                    _management.getVolumeInfoArray();
                for (int index = 0; index < volumes.length; index++)
                {
                    _cachedList.add(
                        volumes[index].getPathName());
                }
            }
            else
            {
                String volumeName = (String)_parent.getSelectedItem();
                if (volumeName == null) return;
                TreeInfo[] trees = 
                    _management.getTreeInfoArray(volumeName);
                for (int index = 0; index < trees.length; index++)
                {
                    _cachedList.add(trees[index].getName());
                }
            }
        }
        catch (Exception e)
        {
            handleException(e);
        }
    }
    
    private void handleException(Exception e)
    {
        e.printStackTrace();
    }
}