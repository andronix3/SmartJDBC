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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionFactory {

    public static Connection getEmbeddedConnection(String user, String pwd, String dbName, File directory, boolean create) throws SQLException {
	Properties props = new Properties();
	if (user != null && !user.isEmpty()) {
	    props.setProperty("user", user);
	    props.setProperty("password", pwd);
	}

	if (directory != null) {
	    System.setProperty("derby.system.home", directory.getAbsolutePath());
	}
	String s = "";
	if (create) {
	    s = ";create=true";
	}
	String protocol = "jdbc:derby:";
	return DriverManager.getConnection(protocol + dbName + s, props);
    }

    public static Connection getPostgreSqlConnection(String database, String user, String pw) throws SQLException {
	return getPostgreSqlConnection("localhost", 5432, database, user, pw);
    }

    public static Connection getPostgreSqlConnection(String host, int port, String database, String user, String pw) throws SQLException {
	try {
	    Class.forName("org.postgresql.Driver");
	} catch (ClassNotFoundException ex) {
	    ex.printStackTrace();
	}

	String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
	Properties props = new Properties();
	props.setProperty("user", user);
	props.setProperty("password", pw);
	
	return DriverManager.getConnection(url, props);
    }

    public static Connection getExcelConnection(File f) throws SQLException {
	try {
	    Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
	} catch (ClassNotFoundException ex) {
	    throw new SQLException(ex);
	}

	String dbName = "Driver={Microsoft Excel Driver (*.xls)};DBQ=" + f.getAbsolutePath() + ";DriverID=22;ReadOnly=true;";
	String url = "jdbc:odbc:" + dbName;
	return DriverManager.getConnection(url, "", "");
    }
}
