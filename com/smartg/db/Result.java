/*
 * Copyright (c) Andrey Kuznetsov. All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  o Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  o Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 *  o Neither the name of imagero Andrey Kuznetsov nor the names of
 *    its contributors may be used to endorse or promote products derived
 *    from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.smartg.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;

/**
 * ResultSet wrapper.
 * 
 * @author andrey
 * 
 */
public class Result implements Enumeration<ArrayList<Object>> {
    private ResultSet rs;
    TableMetadata resultMetadata;
    private int columnCount;
    private boolean hasNext;

    SQLException exception;

    protected Result() {

    }

    Result(ResultSet rs) {
	this.rs = rs;
	if (rs != null) {
	    try {
		resultMetadata = new TableMetadata(rs.getMetaData());
		columnCount = resultMetadata.getColumnCount();
	    } catch (SQLException ex) {
		exception = ex;
		hasNext = false;
	    }
	    next();
	} else {
	    hasNext = false;
	}
    }

    public SQLException getException() {
	return exception;
    }

    public TableMetadata getResultMetadata() {
	return resultMetadata;
    }

    private void next() {
	if (rs != null) {
	    try {
		hasNext = rs.next();
	    } catch (SQLException ex) {
		exception = ex;
		hasNext = false;
	    }
	} else {
	    hasNext = false;
	}
    }

    public boolean hasMoreElements() {
	return hasNext;
    }

    public ArrayList<Object> nextElement() {
	if (hasNext) {
	    ArrayList<Object> list = new ArrayList<Object>();
	    for (int i = 0; i < columnCount; i++) {
		try {
		    list.add(rs.getObject(i + 1));
		} catch (SQLException ex) {
		    list.add(null);
		}
	    }
	    next();
	    return list;
	}
	return null;
    }

    @Override
    protected void finalize() throws Throwable {
	super.finalize();
	rs = null;
    }
}