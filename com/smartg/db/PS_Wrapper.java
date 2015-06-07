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

import java.sql.PreparedStatement;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

class PS_Wrapper {
    PreparedStatement ps;
    String sql;
    Result result;
    boolean valid;
    boolean hasResultSet;

    PS_Wrapper(PreparedStatement ps, String sql) {
	this.ps = ps;
	this.sql = sql;
    }

    void close() {
	if (ps != null) {
	    try {
		ps.close();
	    } catch (SQLException ex) {
		// ignore
	    }
	}
	result = null;
	hasResultSet = false;
	valid = false;
	ps = null;
    }

    boolean execute(Object[] values) throws SQLException {
	return execute(Arrays.asList(values));
    }
    
    boolean execute(Object[] values, Integer[] indexes) throws SQLException {
	return execute(Arrays.asList(values), Arrays.asList(indexes));
    }

    boolean execute(List<Object> values) throws SQLException {
	result = null;
	valid = false;
	if (ps != null && !ps.isClosed()) {
	    int k = 1;
	    for (Object o : values) {
		try {
		    ps.setObject(k++, o);
		}
		catch(SQLDataException e) {
		    System.err.println(o);
		    e.printStackTrace();
		}
	    }
	    hasResultSet = ps.execute();
	    valid = true;
	    return hasResultSet;
	}
	return false;
    }
    
    boolean execute(List<Object> values, List<Integer> indexes) throws SQLException {
	result = null;
	valid = false;
	if (ps != null && !ps.isClosed()) {
	    int k = 0;
	    for (Object o : values) {
		ps.setObject(indexes.get(k++), o);
	    }
	    hasResultSet = ps.execute();
	    valid = true;
	    return hasResultSet;
	}
	return false;
    }

    
    boolean execute() throws SQLException {
	result = null;
	valid = false;
	if (ps != null && !ps.isClosed()) {
	    hasResultSet = ps.execute();
	    valid = true;
	    return hasResultSet;
	}
	return false;
    }

    Result getResult() throws SQLException {
	if (result == null && valid && hasResultSet && ps != null && !ps.isClosed()) {
	    result = new Result(ps.getResultSet());
	}
	return result;
    }

    @Override
    protected void finalize() throws Throwable {
	super.finalize();
	close();
    }
}