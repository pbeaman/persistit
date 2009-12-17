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
 * Created on Aug 17, 2004
 */
package com.persistit;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.util.Vector;

/**
 * Parses String values as Key or KeyFilter values.
 *
 * @version 1.0
 */
public class KeyParser
{
    private String _source;
    private StringBuffer _sb = new StringBuffer();
    boolean _dp;
    private int _index;
    private int _start;
    private int _end;
    
    private static class ProtoTerm
    {
        byte[] _leftBytes;
        byte[] _rightBytes;
        boolean _leftInclusive;
        boolean _rightInclusive;
        
        private KeyFilter.Term term()
        {
            if (_rightBytes == null)
            {
                return new KeyFilter.SimpleTerm(_leftBytes);
            }
            else
            {
                return new KeyFilter.RangeTerm(
                _leftBytes,
                _rightBytes,
                _leftInclusive,
                _rightInclusive);
            }
        }
    }
    
    /**
     * Constructs a <tt>KeyParser</tt> for the specified string.
     * @param source    The string to be parsed.
     */
    public KeyParser(String source)
    {
        _source = source;
        _index = 0;
        _start = 0;
        _end = source.length();
    }

    /**
     * Constructs a <tt>KeyParser</tt> for a substring of the specified string.
     * 
     * @param source    The string
     * 
     * @param offset    Offset of the first character of the substring to parse
     * 
     * @param size      Size of the substring
     */
    public KeyParser(String source, int offset, int size)
    {
        if (offset < 0 || offset + size > source.length())
        {
            throw new IllegalArgumentException();
        }
        _source = source;
        _start = offset;
        _end = offset + size;
        _index = offset;
    }
    
    /**
     * Parses a key value from the string or substring from which
     * this <tt>KeyParser</tt> was constructed, modifying the
     * supplied <tt>Key</tt> to contain the result.
     * 
     * @return  <tt>true</tt> if the key value was parsed successfully.
     *          <tt>false</tt> if the string or substring was
     *          invalid as a <a href="Key.html#_stringRepresentation"> string
     *          representation</a> of a <tt>Key</tt>.
     */
    public boolean parseKey(Key key)
    {
        int index = _index;
        boolean result = false;
        boolean first = true;
        
        try
        {
            if (matchNonWhiteChar('{'))
            {
                for(;;)
                {
                    int c = getNonWhiteChar();
                    if (c == '}')
                    {
                        result = true;
                        break;
                    }
                    if (first) back();
                    else if (c != ',') break;
                    
                    if (!parseKeySegment(key)) break;
                    first = false;
                }
                if (result) result = getNonWhiteChar() == -1;
            }
        }
        finally
        {
            if (!result) _index = index;
        }
        return result;
    }
    
    /**
     * Parses and returns a <tt>KeyFilter</tt> from the string or substring 
     * from which this <tt>KeyParser</tt> was constructed.
     * 
     * @return  A <tt>KeyFilter</tt> or <tt>null</tt> if the string or 
     *          substring was invalid as a 
     *          <a href="KeyFilter.html#_stringRepresentation"> string
     *          representation</a> of a <tt>KeyFilter</tt>.
     */
    public KeyFilter parseKeyFilter()
    {
        int saveIndex = _index;
        Vector vector = new Vector();   // accumulate Terms
        boolean result = false;
        boolean minDepthSet = false;
        boolean maxDepthSet = false;
        int minDepth = 0;
        int maxDepth = Integer.MAX_VALUE;
        Key workKey = new Key((Persistit)null);
        ProtoTerm protoTerm = new ProtoTerm();
        int depth = 0;
        try
        {
            if (matchNonWhiteChar('{'))
            {
                for(;;)
                {
                    int c = getNonWhiteChar();
                    if (c == '}')
                    {
                        result = true;
                        break;
                    }
                    if (depth == 0) back();
                    else
                    {
                        if (c != ',') break;
                    }
                    c = getNonWhiteChar();
                    if (c == '>' && !minDepthSet)
                    {
                        minDepth = depth + 1;
                        minDepthSet = true;
                        c = getNonWhiteChar();
                    }
                    if (c == '{')
                    {
                        KeyFilter.Term[] array = parseFilterTermArray(workKey, protoTerm);
                        if (array == null || !matchNonWhiteChar('}')) break;
                        else vector.add(KeyFilter.orTerm(array));
                    }
                    else
                    {
                        back();
                        KeyFilter.Term term = parseFilterTerm(workKey, protoTerm);
                        if (term == null) break;
                        vector.add(term);
                    }
                    if (matchNonWhiteChar('<') && !maxDepthSet)
                    {
                        maxDepth = depth + 1;
                        maxDepthSet = true;
                    }
                    depth++;
                }
                if (result) result = getNonWhiteChar() == -1;
            }
        }
        finally
        {
            if (!result) _index = saveIndex;
        }
        if (!result) return null;
        KeyFilter.Term[] terms = new KeyFilter.Term[vector.size()];
        for (int index = 0; index < terms.length; index++)
        {
            terms[index] = (KeyFilter.Term)vector.get(index);
        }
        return new KeyFilter(terms, minDepth, maxDepth);
    }
    
    /**
     * Return the current index for parsing.  This indicates the first
     * character that did not conform
     * 
     * @return  The index
     */
    public int getIndex()
    {
        return _index;
    }
    /**
     * Attempts to parse a chunk of the source string as a
     * key segment. If successful, appends the segment to the key.
     * @param key
     * @return
     */
    private boolean parseKeySegment(Key key)
    {
        int index = _index;
        int size = key.getEncodedSize();
        boolean result = false;
        try
        {
            int c = getNonWhiteChar();
            if (c == '\"' && matchQuotedStringTail())
            {
                result = true;
                key.append(_sb);
            }
            else if (c == '(')
            {
                back();
                if (matchExactString(Key.PREFIX_BOOLEAN))
                {
                    result = true;
                    if (matchExactString("true")) key.append(true);
                    else if (matchExactString("false")) key.append(false);
                    else result = false;
                }
                else if (matchExactString(Key.PREFIX_BYTE))
                {
                    if (matchNumber(false, false))
                    {
                        key.append(Byte.parseByte(_sb.toString()));
                        result = true;
                    }
                }
                else if (matchExactString(Key.PREFIX_SHORT))
                {
                    if (matchNumber(false, false))
                    {
                        key.append(Short.parseShort(_sb.toString()));
                        result = true;
                    }
                }
                else if (matchExactString(Key.PREFIX_CHAR))
                {
                    if (matchNumber(false, false))
                    {
                        key.append((char)Integer.parseInt(_sb.toString()));
                        result = true;
                    }
                }
                else if (matchExactString(Key.PREFIX_INT))
                {
                    if (matchNumber(false, false))
                    {
                        key.append(Integer.parseInt(_sb.toString()));
                        result = true;
                    }
                }
                else if (matchExactString(Key.PREFIX_LONG))
                {
                    if (matchNumber(false, false))
                    {
                        key.append(Long.parseLong(_sb.toString()));
                        result = true;
                    }
                }
                else if (matchExactString(Key.PREFIX_FLOAT))
                {
                    if (matchNumber(true, false))
                    {
                        key.append(Float.parseFloat(_sb.toString()));
                        result = true;
                    }
                }
                else if (matchExactString(Key.PREFIX_DOUBLE))
                {
                    if (matchNumber(true, false))
                    {
                        key.append(Double.parseDouble(_sb.toString()));
                        result = true;
                    }
                }
                else if (matchExactString(Key.PREFIX_BIG_INTEGER) ||
                         matchExactString(Key.PREFIX_BIG_INTEGER0))
                {
                    if (matchNumber(false, false))
                    {
                        key.append(new BigInteger(_sb.toString()));
                        result = true;
                    }
                }
                else if (matchExactString(Key.PREFIX_BIG_DECIMAL) ||
                         matchExactString(Key.PREFIX_BIG_DECIMAL0))
                {
                    if (matchNumber(true, false))
                    {
                        key.append(new BigDecimal(_sb.toString()));
                        result = true;
                    }
                }
                else if (matchExactString(Key.PREFIX_STRING) ||
                         matchExactString(Key.PREFIX_STRING0))
                {
                    c = getNonWhiteChar();
                    if (c == '\"' && matchQuotedStringTail())
                    {
                        key.append(_sb);
                        result = true;
                    }
                }
                else if (matchExactString(Key.PREFIX_DATE) ||
                         matchExactString(Key.PREFIX_DATE0))
                {
                    if (matchNumber(true, true))
                    {
                        key.append(Key.SDF.parse(_sb.toString()));
                        result = true;
                    }
                }
                else if (matchExactString(Key.PREFIX_BYTE_ARRAY))
                {
                    if (matchUntil(',', '}'))
                    {
                        back();
                        key.append(Util.hexToBytes(_sb.toString()));
                        result = true;
                    }
                }
            }
            else
            {
                back();
                if (matchNumber(true, false))
                {
                    if (_dp) key.append(Double.parseDouble(_sb.toString()));
                    else key.append(Integer.parseInt(_sb.toString()));
                    result = true;
                }
                else if (matchExactString("null"))
                {
                    key.append(null);
                    result = true;
                }
                else if (matchExactString("true"))
                {
                    key.append(true);
                    result = true;
                }
                else if (matchExactString("false"))
                {
                    key.append(false);
                    result = true;
                }
            }
        }
        catch (NumberFormatException nfe)
        {
        }
        catch (ParseException pe)
        {
        }
        finally
        {
            if (!result)
            {
                _index = index;
                key.setEncodedSize(size);
            }
        }
        return result;
    }
    
    private KeyFilter.Term[] parseFilterTermArray(Key workKey, ProtoTerm protoTerm)
    {
        Vector vector = new Vector();
        for (;;)
        {
            KeyFilter.Term term = parseFilterTerm(workKey, protoTerm);
            if (term == null) return null;
            vector.add(term);
            int c = getNonWhiteChar();
            if (c == '}')
            {
                back();
                KeyFilter.Term[] array = new KeyFilter.Term[vector.size()];
                for (int index = 0; index < array.length; index++)
                {
                    array[index] = (KeyFilter.Term)vector.get(index);
                }
                return array;
            }
            else if (c != ',') return null;
        }
    }
    
    private KeyFilter.Term parseFilterTerm(Key workKey, ProtoTerm protoTerm)
    {
        int c = getNonWhiteChar();
        if (c == '*')
        {
            return KeyFilter.ALL;
        }
        back();
        
        protoTerm._leftBytes = null;
        protoTerm._rightBytes = null;
        protoTerm._leftInclusive = true;
        protoTerm._rightInclusive = true;
        
        KeyFilter.Term term = null;
        boolean okay = parseKeyFilterRange(workKey, protoTerm);
        if (okay) term = protoTerm.term();
        else
        {
            c = getNonWhiteChar();
            if (c == '(' || c == '[')
            {
                protoTerm._leftInclusive = (c == '[');
                okay = parseKeyFilterRange(workKey, protoTerm);
                if (okay)
                {
                    c = getNonWhiteChar();
                    if (c != ')' && c != ']') okay = false;
                    else
                    {
                        protoTerm._rightInclusive = (c == ']');
                        term = protoTerm.term();
                    }
                }
            }
        }
        return term;
    }
    
    boolean parseKeyFilterRange(Key workKey, ProtoTerm protoTerm)
    {
        
        workKey.clear();
        int c = getNonWhiteChar();
        back();
        if (c == ':')
        {
            workKey.append(Key.BEFORE);
        }
        else
        {
            if (!parseKeySegment(workKey)) return false;
        }
        
        protoTerm._leftBytes = segmentBytes(workKey);
        protoTerm._rightBytes = null;
        
        if (matchNonWhiteChar(':'))
        {
            workKey.clear();
            c = getNonWhiteChar();
            back();
            if (c == ')' || c == ']' || c == ',' || c == '}' || c == -1)
            {
                workKey.append(Key.AFTER);
            }
            else
            {
                if (!parseKeySegment(workKey)) return false;
            }
            protoTerm._rightBytes = segmentBytes(workKey);
        }
        return true;
    }
    
    static byte[] segmentBytes(Key key)
    {
        int start = key.getIndex();
        int end = key.nextElementIndex(start);
        if (end == -1) end = key.getEncodedSize();
        byte[] bytes = new byte[end - start];
        System.arraycopy(key.getEncodedBytes(), start, bytes, 0, end - start);
        return bytes;
    }
    
    private int getNonWhiteChar()
    {
        int c = getChar();
        while (c == ' ' || c == '\t'  || c == '\n' || c == '\r')
        {
            c = getChar();
        }
        return c;
    }
    
    private boolean matchNonWhiteChar(int c)
    {
        return getNonWhiteChar() == c ? true : back();
    }

    private boolean matchUntil(int c1, int c2)
    {
        _sb.setLength(0);
        int c;
        while ((c = getChar()) != -1)
        {
            if (c == c1 || c == c2) return true;
            else _sb.append((char)c);
        }
        return false;
    }
    
    private boolean matchNumber(boolean decimalPoint, boolean tzsign)
    {
        _sb.setLength(0);
        _dp = false;
        int c = getNonWhiteChar();
        if (c != '-' && (c < '0' || c > '9') && (c != '.' || !decimalPoint))
        {
            back();
            return false;
        }
        _sb.append((char)c);
        if (c == '.')
        {
            decimalPoint = false;
            _dp = true;
        }
        while ((c = getChar()) >= '0' && c <= '9' ||
               (decimalPoint && c == '.') || 
               (tzsign & (c == '-' || c == '+')))
        {
            _sb.append((char)c);
            if (c == '.')
            {
                decimalPoint = false;
                _dp = true;
            }
            if (c == '-' || c == '+') tzsign = false;
        }
        back();
        return true;
    }
    
    private boolean matchExactString(String s)
    {
        if (_source.regionMatches(_index, s, 0, s.length()))
        {
            _index += s.length();
            return true;
        }
        return false;
    }
    
    private boolean matchQuotedStringTail()
    {
        boolean escape = false;
        _sb.setLength(0);
        for (;;)
        {
            int c = getChar();
            if (c == -1) return false;
            if (c == '\\')
            {
                if (escape) _sb.append('\\');
                escape = !escape;
            }
            else if (escape)
            {
                switch(c)
                {
                    case 'r':
                        c = '\r';
                        break;
                    case 'n':
                        c = '\n';
                        break;
                    case 'f':
                        c = '\f';
                        break;
                    case 'b':
                        c = '\b';
                        break;
                    case 't':
                        c = '\t';
                        break;
                    case 'u':
                        c = unicode();
                        if (c == -1) return false;
                        break;
                    default:
                }
                _sb.append((char)c);
                escape = false;
            }
            else
            {
                if (c == '\\') escape = true;
                else if (c == '\"')
                {
                    return true;
                }
                else _sb.append((char)c);
            }
        }
    }

    private int unicode()
    {
        if (_index + 4 > _end) return -1;
        int u = Integer.parseInt(_source.substring(_index, _index + 4), 16);
        _index += 4;
        return u;
    }

    private int getChar()
    {
        if (_index >= 0 && _index < _end) return _source.charAt(_index++);
        else return -1;
    }

    private boolean back()
    {
        if (_index > _start) _index--;
        return false;
    }
    
}
