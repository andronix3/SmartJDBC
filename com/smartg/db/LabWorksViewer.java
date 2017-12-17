package com.smartg.db;

import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.sql.Connection;
import java.sql.SQLException;

import javax.swing.JFrame;

import labworksUtils.LabworksConnectionException;
import labworksUtils.LabworksUtils;

public class LabWorksViewer {

	public static void main(String... args) throws SQLException, LabworksConnectionException {
		Connection conn = LabworksUtils.getINSTANCE().getLabworksDB().getConnection();
		JavaDB_Connection frontEnd = new JavaDB_Connection(conn);

		DB_Viewer viewer = new DB_Viewer(frontEnd);

		JFrame frame = new JFrame();
		frame.getContentPane().add(viewer);
		frame.pack();
		frame.setVisible(true);

		frame.addWindowListener(new WindowAdapter() {
			@Override
			public void windowClosing(WindowEvent e) {
				System.exit(0);
			}
		});
	}
}
