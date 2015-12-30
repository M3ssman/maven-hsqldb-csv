package info.bitsrc.embedded_hsqldb;

import static org.junit.Assert.*;
import info.bitsrc.embedded_hsqldb.OldSchoolDBService;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * 
 * @author u.hartwig
 *
 */
public class OldSchoolDBServiceTest {

	private String file1 = "/data/tab_karte.csv";

	private String file2 = "/data/tab_karte_typ.csv";

	static OldSchoolDBService dbs;

	private String sqlAbove50 = "SELECT "
	        + "  Nachname"
	        + ", Strasse"
	        + ", SUM( Betrag ) AS Gesamt"
	        + ", COUNT( ID_Auftrag ) AS Anz"
	        + " FROM HSQLDB_EBEDDED.Kunde K JOIN HSQLDB_EBEDDED.Auftrag A ON A.ID_Kunde = K.ID_Kunde "
	        + "WHERE "
	        + " Datum > {D '%s' } AND Datum < {D '%s' } "
	        + "GROUP BY Nachname, Strasse "
	        + "HAVING ( ( SUM( Betrag ) > 49.99 ) ) "
	        + "ORDER BY Gesamt DESC";

	private String totalPerYear = "SELECT "
	        + " SUM( A.Betrag ) AS Gesamt, COUNT( A.ID_Auftrag ) AS Anz"
	        + " FROM HSQLDB_EBEDDED.Auftrag A "
	        + "WHERE A.Datum > {D '%s' } AND A.Datum < {D '%s' } ";

	@BeforeClass
	public static void setUpBefore() {
		dbs = new OldSchoolDBService();
		assertNotNull(dbs);
	}

	@Test
	public void testExistenceFile1() {
		assertNotNull("Test file '" + file1 + "' missing", getClass().getResource(file1));
	}

	@Test
	public void testExistenceFile2() {
		assertNotNull("Test file '" + file2 + "' missing", getClass().getResource(file2));
	}

	@Test
	public void testExecute2007() {
		System.out.println("### 2007");
		try {
			String sql = String.format(sqlAbove50, "2006-11-30", "2007-12-01");
			ResultSet rs = dbs.execute(sql);
			int i = 0;
			while (rs.next()) {
				i++;
			}
			System.out.println("Insgesamt " + i + " Kandidaten über 50");

			sql = String.format(totalPerYear, "2006-11-30", "2007-12-01");
			rs = dbs.execute(sql);
			while (rs.next()) {
				System.out.println("Umsatz: " + String.format("%.2f", rs.getFloat(1)) + "\tAufträge: "
				        + rs.getNString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testExecute2008() {
		System.out.println("### 2008");
		try {
			String sql = String.format(sqlAbove50, "2007-11-30", "2008-12-01");
			ResultSet rs = dbs.execute(sql);
			int i = 0;
			while (rs.next()) {
				i++;
			}
			System.out.println("Insgesamt " + i + " Kandidaten über 50");

			sql = String.format(totalPerYear, "2007-11-30", "2008-12-01");
			rs = dbs.execute(sql);
			while (rs.next()) {
				System.out.println("Umsatz: " + String.format("%.2f", rs.getFloat(1)) + "\tAufträge: "
				        + rs.getNString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testExecute2009() {
		assertNotNull(dbs);
		String sql = String.format(sqlAbove50, "2008-11-30", "2009-12-01");
		ResultSet rs = dbs.execute(sql);
		assertNotNull(rs);
		try {
			int i = 0;
			System.out.println("### 2009");
			while (rs.next()) {
				i++;
			}
			System.out.println("Insgesamt " + i + " Kandidaten über 50");

			sql = String.format(totalPerYear, "2008-11-30", "2009-12-01");
			rs = dbs.execute(sql);
			while (rs.next()) {
				System.out.println("Umsatz: " + String.format("%.2f", rs.getFloat(1)) + "\tAufträge: "
				        + rs.getNString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testExecute2010() {
		assertNotNull(dbs);
		String sql = String.format(sqlAbove50, "2009-11-30", "2010-12-01");
		ResultSet rs = dbs.execute(sql);
		assertNotNull(rs);
		try {
			int i = 0;
			System.out.println("### 2010");
			while (rs.next()) {
				i++;
				// System.out.println(i+"\t"+rs.getNString(3)+"\t"+rs.getNString(1)+"\t"+rs.getNString(2));
			}
			System.out.println("Insgesamt " + i + " Kandidaten über 50");

			sql = String.format(totalPerYear, "2009-11-30", "2010-12-01");
			rs = dbs.execute(sql);
			while (rs.next()) {
				System.out.println("Umsatz: " + String.format("%.2f", rs.getFloat(1)) + "\tAufträge: "
				        + rs.getNString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testExecute2011() {
		assertNotNull(dbs);
		String sql = String.format(sqlAbove50, "2010-11-30", "2011-12-01");
		ResultSet rs = dbs.execute(sql);
		assertNotNull(rs);
		try {
			int i = 0;
			System.out.println("### 2011");
			while (rs.next()) {
				i++;
				// System.out.println(i+"\t"+rs.getNString(3)+"\t"+rs.getNString(1)+"\t"+rs.getNString(2));
			}
			System.out.println("Insgesamt " + i + " Kandidaten über 50");

			sql = String.format(totalPerYear, "2010-11-30", "2011-12-01");
			rs = dbs.execute(sql);
			while (rs.next()) {
				System.out.println("Umsatz: " + String.format("%.2f", rs.getFloat(1)) + "\tAufträge: "
				        + rs.getNString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testExecute2012() {
		ResultSet rs = dbs.execute(String.format(sqlAbove50, "2011-11-30", "2012-12-01"));
		assertNotNull(rs);
		try {
			int i = 0;
			System.out.println("### 2012");
			while (rs.next()) {
				i++;
				// System.out.println(i+"\t"+rs.getNString(3)+"\t"+rs.getNString(1)+"\t"+rs.getNString(2));
			}
			System.out.println("Insgesamt " + i + " Kandidaten über 50");

			rs = dbs.execute(String.format(totalPerYear, "2011-11-30", "2012-12-01"));
			while (rs.next()) {
				System.out.println("Umsatz: " + String.format("%.2f", rs.getFloat(1)) + "\tAufträge: "
				        + rs.getNString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testExecute2013() {
		ResultSet rs = dbs.execute(String.format(sqlAbove50, "2012-11-30", "2013-12-01"));
		assertNotNull(rs);
		try {
			int i = 0;
			System.out.println("### 2013");
			while (rs.next()) {
				i++;
				// System.out.println(i+"\t"+rs.getNString(3)+"\t"+rs.getNString(1)+"\t"+rs.getNString(2));
			}
			System.out.println("Insgesamt " + i + " Kandidaten über 50");

			rs = dbs.execute(String.format(totalPerYear, "2012-11-30", "2013-12-01"));
			while (rs.next()) {
				System.out.println("Umsatz: " + String.format("%.2f", rs.getFloat(1)) + "\tAufträge: "
				        + rs.getNString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testExecute2014() {
		assertNotNull(dbs);
		ResultSet rs = dbs.execute(String.format(sqlAbove50, "2013-11-30", "2014-12-01"));
		assertNotNull(rs);
		try {
			int i = 0;
			System.out.println("### 2014");
			while (rs.next()) {
				i++;
				// System.out.println(i+"\t"+rs.getNString(3)+"\t"+rs.getNString(1)+"\t"+rs.getNString(2));
			}
			System.out.println("Insgesamt " + i + " Kandidaten über 50");

			rs = dbs.execute(String.format(totalPerYear, "2013-11-30", "2014-12-01"));
			while (rs.next()) {
				System.out.println("Umsatz: " + String.format("%.2f", rs.getFloat(1)) + "\tAufträge: "
				        + rs.getNString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testExecute2015() {
		assertNotNull(dbs);
		String sql = "SELECT "
		        + "  K.Nachname"
		        + ", K.Strasse"
		        + ", SUM( A.Betrag ) AS Gesamt"
		        + ", COUNT( A.ID_Auftrag ) AS Anz"
		        + " FROM HSQLDB_EBEDDED.Kunde K JOIN HSQLDB_EBEDDED.Auftrag A ON A.ID_Kunde = K.ID_Kunde "
		        + "WHERE "
		        + " A.Datum > {D '2014-11-30' } "
		        + "AND A.Datum < {D '2015-12-01' } "
		        + "GROUP BY K.Nachname, K.Strasse "
		        + "HAVING ( ( SUM( A.Betrag ) > 49.99 ) ) "
		        + "ORDER BY Gesamt DESC";

		ResultSet rs = dbs.execute(sql);
		assertNotNull(rs);
		try {
			int i = 0;
			System.out.println("### 2015");
			while (rs.next()) {
				i++;
				// System.out.println(i+"\t"+rs.getNString(3)+"\t"+rs.getNString(1)+"\t"+rs.getNString(2));
			}
			System.out.println("Insgesamt " + i + " Kandidaten über 50");

			sql = String.format(totalPerYear, "2014-11-30", "2015-12-01");
			rs = dbs.execute(sql);
			while (rs.next()) {
				System.out.println("Umsatz: " + String.format("%.2f", rs.getFloat(1)) + "\tAufträge: "
				        + rs.getNString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testExecute2() {
		assertNotNull(dbs);
		ResultSet rs = dbs
		        .execute("SELECT "
		                + "Anrede, Nachname, Strasse AS \"Straße\", Ort, PLZ, "
		                + "SUM( Betrag ) AS Gesamtbetrag, "
		                + "COUNT( ID_Auftrag ) AS \"Anzahl Aufträge\" "
		                // "Karten"."wk_2005" AS "WK 05",
		                // "Karten"."wk_2006" AS "Wk 06",
		                // "Karten"."wk_2007" AS "WK 07"
		                + "FROM "
		                + "{ OJ HSQLDB_EBEDDED.Kunde LEFT OUTER JOIN HSQLDB_EBEDDED.Karte ON Kunde.ID_Kunde = Karte.ID_Kunde }, "
		                + "HSQLDB_EBEDDED.Auftrag "
		                + "WHERE Auftrag.ID_Kunde = Kunde.ID_Kunde AND Datum > {D '2006-11-06' } AND Datum < {D '2007-12-01' } "
		                + "GROUP BY Anrede, Nachname, Strasse, Ort, PLZ "
		                // +
						// ""Karten"."wk_2003", "Karten"."wk_2004", "Karten"."wk_2005", "Karten"."wk_2006", "Karten"."wk_2007" "
		                + "HAVING ( ( SUM( Betrag ) > 49.99 ) ) "
		                + "ORDER BY Nachname ASC");
		assertNotNull(rs);
		try {
			while (rs.next()) {
				System.out.println(rs.getNString(1) + " " + rs.getNString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// Anrede VARCHAR(16), Nachname VARCHAR(32), Strasse VARCHAR(32), Ort VARCHAR(32), PLZ VARCHAR(8)
	@Test
	public void testInsertKunde() {
		String sqlInsert = "INSERT INTO HSQLDB_EBEDDED.KUNDE (ID_Kunde, Anrede, Nachname, Strasse, Ort, PLZ) VALUES (DEFAULT, 'Herr', 'Mustermann', 'Rübenstraße 88','Musterstadt','00100');";
		for (int i = 0; i < 1_000; i++) {
			int rs = dbs.insert(sqlInsert);
			assertNotEquals(0, rs);
		}
	}

	@AfterClass
	public static void shutDown() throws SQLException {
		dbs.c.commit();
		dbs.c.close();
	}

}
