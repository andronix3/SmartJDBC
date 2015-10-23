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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

public class JavaDB_Connection extends JDB_Actions {

    public JavaDB_Connection(Connection connection) throws SQLException {
	super(connection);
    }

    public SQLException getLastError() {
	return lastError;
    }

    /**
     * execute SQL command.
     * 
     * @param sql
     * @return true if execution was successful and returned ResultSet, check
     *         getLastError.
     */
    public boolean execute(String sql) {
	return new ExecuteSQL(sql).execute().success;
    }

    /**
     * Create prepared statement.
     * 
     * @param name
     *            statement name (can be used to access it)
     * @param sql
     */
    public void prepareStatement(String name, String sql) {
	statementDefs.put(name, sql);
    }

    public boolean hasStatement(String name) {
	return statementDefs.get(name) != null;
    }

    public void createType(String name, Class<?> javaClass) {
	StringBuffer sb = new StringBuffer("create type ");
	sb.append(name);
	sb.append(" EXTERNAL NAME '");
	sb.append(javaClass.getName());
	sb.append("' LANGUAGE JAVA");

	String sql = sb.toString();
	execute(sql);
    }

    public boolean createTable(TableDef td) {
	StringBuffer sb = new StringBuffer("create table ");
	sb.append(td.tableName);
	sb.append(" (");

	Enumeration<Column> colenum = td.columns();
	while (colenum.hasMoreElements()) {
	    Column c = colenum.nextElement();
	    sb.append(c.toString());
	    if (colenum.hasMoreElements()) {
		sb.append(",");
	    }
	}

	sb.append(")");

	String sql = sb.toString();
	boolean success = execute(sql);
	if (success) {
	    colenum = td.columns();
	    while (colenum.hasMoreElements()) {
		Column c = colenum.nextElement();
		if (c.needIndex()) {
		    sql = "CREATE INDEX INDEX_" + td.tableName + "_" + c.getName() + " ON " + td.tableName + " (" + c.getName() + ")";
		    execute(sql);
		}
	    }
	}
	return success;
    }

    public void dropTable(String name) {
	execute("drop table " + name);
    }

    /**
     * Get table metadata. Check getLastError if returned object was null.
     * 
     * @param tableName
     * @return TableMetadata or null if table not exists or error occured.
     */
    public TableMetadata getTableMetadata(String tableName) {
	TableMetadata tm = tableMetadata.get(tableName);
	if (tm == null) {
	    RefreshTableMetadata a = new RefreshTableMetadata(tableName);
	    tm = a.execute().result;
	}
	return tm;
    }

    /**
     * Call it after you changed table structure (added or remove column, etc.)
     * 
     * @param tableName
     * @return
     */
    public boolean refreshTableMetadata(String tableName) {
	RefreshTableMetadata a = new RefreshTableMetadata(tableName);
	return a.success();
    }

    /**
     * Execute prepared statement.
     * 
     * @param name
     *            statement name
     * @param values
     *            values for statement.
     * @return
     * @throws SQLException
     */
    public boolean execute(String name, List<Object> values) throws SQLException {
	PS_Wrapper psWrapper = get(name);
	return psWrapper.execute(values);
    }

    public boolean execute(String name, List<Object> values, List<Integer> indexes) throws SQLException {
	PS_Wrapper psWrapper = get(name);
	return psWrapper.execute(values, indexes);
    }

    /**
     * Execute prepared statement.
     * 
     * @param name
     *            statement name
     * @param values
     *            values for statement.
     * @return
     * @throws SQLException
     */
    public boolean execute(String name, Object... values) throws SQLException {
	List<Object> asList = Arrays.asList(values);
	return execute(name, asList);
    }

    public boolean execute(String name, Object[] values, Integer[] indexes) throws SQLException {
	return execute(name, Arrays.asList(values), Arrays.asList(indexes));
    }

    public boolean execute2(String name) throws SQLException {
	PS_Wrapper psWrapper = get(name);
	return psWrapper.execute();
    }

    /**
     * call count(*) function
     * 
     * @param table
     *            table name
     * @param condition
     * @return
     * @throws SQLException
     */
    public int count(String table, String condition) throws SQLException {
	String sql = "select count(*) from " + table + " where " + condition;
	synchronized (defaultStatement) {
	    defaultStatement.execute(sql);
	    ResultSet rs1 = defaultStatement.getResultSet();
	    rs1.next();
	    int k = rs1.getInt(1);
	    rs1.close();
	    return k;
	}
    }

    public boolean checkCondition(String table, String condition) throws SQLException {
	return count(table, condition) > 0;
    }

    private String url;

    /**
     * get database URL
     * 
     * @return String or null (check getLastError in this case)
     */
    public String getURL() {
	if (url == null) {
	    try {
		DatabaseMetaData metadata = getDatabaseMetaData();
		url = metadata.getURL();
	    } catch (SQLException ex) {
		lastError = ex;
	    }
	}
	return url;
    }

    boolean isExcelConnection() {
	String url = getURL();
	if (url.indexOf("Excel") >= 0) {
	    return true;
	}
	if (url.indexOf("*.xls") >= 0) {
	    return true;
	}
	return false;
    }

    /**
     * get database tables
     * 
     * @return Result or null (check getLastError() in latter case)
     */
    public Result getTables() {
	Result result = null;
	DatabaseMetaData metadata;
	try {
	    metadata = getDatabaseMetaData();
	    ResultSet tables = metadata.getTables(null, null, null, null);
	    result = new Result(tables);
	} catch (SQLException ex) {
	    lastError = ex;
	}
	return result;
    }

    public boolean hasTable(String name) {
	Result tables = getTables();
	while (tables.hasMoreElements()) {
	    ArrayList<Object> next = tables.nextElement();
	    if (name.equalsIgnoreCase(next.get(2).toString())) {
		return true;
	    }
	}
	return false;
    }

    public void printTables() {
	Result tables = getTables();
	if (tables != null) {
	    while (tables.hasMoreElements()) {
		ArrayList<Object> next = tables.nextElement();
		System.out.println(next);
	    }
	}
	System.out.println("----------------------------------");
    }

    @Override
    protected void finalize() throws Throwable {
	super.finalize();
	closeConnection();
    }

    public Result getResult(String key) {
	ActionResult<Result> execute = new GetResultSet(key).execute();
	return execute.result;
    }
}
