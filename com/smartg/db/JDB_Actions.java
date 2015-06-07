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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.logging.Logger;

public class JDB_Actions {

    protected HashMap<String, PS_Wrapper> statements = new HashMap<String, PS_Wrapper>();
    protected HashMap<String, String> statementDefs = new HashMap<String, String>();
    protected HashMap<String, TableMetadata> tableMetadata = new HashMap<String, TableMetadata>();

    protected SQLException lastError;

    protected final Statement defaultStatement;

    private final Connection connection;

    protected JDB_Actions(Connection connection) throws SQLException {
	this.connection = connection;
	defaultStatement = connection.createStatement();
    }

    protected PreparedStatement prepareStatement(String sql) throws SQLException {
	return connection.prepareStatement(sql);
    }

    protected DatabaseMetaData getDatabaseMetaData() throws SQLException {
	return connection.getMetaData();
    }

    public void closeConnection() {
	Collection<PS_Wrapper> values = statements.values();
	for (PS_Wrapper ps : values) {
	    ps.close();
	}
	try {
	    connection.close();
	} catch (SQLException ex) {
	    printSQLException(ex, true);
	}
	statements.clear();
    }

    public class ExecuteSQL extends JDB_Action<Boolean> {
	String sql;

	ExecuteSQL(String sql) {
	    this.sql = sql;
	}

	@Override
	protected Boolean dbAction() throws SQLException {
	    Logger.getLogger(getClass().getName()).info(sql);
	    return defaultStatement.execute(sql);
	}
    }

    public class RefreshTableMetadata extends JDB_Action<TableMetadata> {

	private String tableName;

	public RefreshTableMetadata(String tableName) {
	    this.tableName = tableName;
	}

	@Override
	protected TableMetadata dbAction() throws SQLException {
	    Result result = executeAndGetResult("SELECT * FROM " + tableName + " FETCH FIRST ROW ONLY");
	    if (result != null) {
		TableMetadata tm = result.getResultMetadata();
		tableMetadata.put(tableName, tm);
		return tm;
	    }
	    return null;
	}

	boolean success() {
	    return execute() != null;
	}
    }

    public class GetResultSet extends JDB_Action<Result> {
	private String key;

	public GetResultSet(String key) {
	    this.key = key;
	}

	@Override
	protected Result dbAction() throws SQLException {
	    return JDB_Actions.this.getResult0(key);
	}
    }

    /**
     * Execute sql expression and get result
     * 
     * @param sql
     * @return Result or null (check getLastError() in latter case).
     */
    public synchronized Result executeAndGetResult(String sql) {
	ExecuteSQL esql = new ExecuteSQL(sql);
	ActionResult<Boolean> execute = esql.execute();
	if (execute != null && execute.result != null && execute.result) {
	    return new GetResultSet(null).execute().result;
	}
	return null;
    }

    protected PS_Wrapper get(String name) throws SQLException {
	PS_Wrapper psWrapper = statements.get(name);
	if (psWrapper == null) {
	    String sdef = statementDefs.get(name);
	    if (sdef != null) {
		psWrapper = prepareStatement0(name, sdef);
	    } else {
		throw new SQLException("No such statement: " + name);
	    }
	}
	return psWrapper;
    }

    private Result getResult0(String name) throws SQLException {
	if (name == null) {
	    return new Result(defaultStatement.getResultSet());
	}
	PS_Wrapper psWrapper = get(name);
	return psWrapper.getResult();
    }

    private PS_Wrapper prepareStatement0(String name, String sql) throws SQLException {
	PS_Wrapper s = statements.get(name);
	if (s != null && s.sql.equalsIgnoreCase(sql)) {
	    System.err.println("PreparedStatement already exists: " + sql);
	    return s;
	}
	PreparedStatement ps = prepareStatement(sql);
	PS_Wrapper psw = new PS_Wrapper(ps, sql);
	statements.put(name, psw);
	return psw;
    }

    public static void printSQLException(SQLException ex, boolean printStackTraces) {
	while (ex != null) {
	    System.err.println("\n----- SQLException -----"); //$NON-NLS-1$
	    System.err.println("  SQL State:  " + ex.getSQLState()); //$NON-NLS-1$
	    System.err.println("  Error Code: " + ex.getErrorCode()); //$NON-NLS-1$
	    System.err.println("  Message:    " + ex.getMessage()); //$NON-NLS-1$
	    if (printStackTraces) {
		ex.printStackTrace();
	    }
	    ex = ex.getNextException();
	}
    }
}
