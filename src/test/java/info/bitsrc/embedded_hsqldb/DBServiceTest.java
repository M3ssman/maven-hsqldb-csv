package info.bitsrc.embedded_hsqldb;

import static org.junit.Assert.*;
import info.bitsrc.embedded_hsqldb.DBService;

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
public class DBServiceTest {

	private String file1 = "/data/tab_karte.csv";

	private String file2 = "/data/tab_karte_typ.csv";
	
	static DBService dbs;

	private String sqlAbove50 = "SELECT "
			+ "  K.Nachname"
			+ ", K.Strasse"
			+ ", SUM( A.Betrag ) AS Gesamt"
			+ ", COUNT( A.ID_Auftrag ) AS Anz"
			+ " FROM Kunde K JOIN Auftrag A ON A.Kunde = K.ID_Kunde "
			+ "WHERE "
			+ " A.Datum > {D '%s' } "
			+ "AND A.Datum < {D '%s' } "
			+ "GROUP BY K.Nachname, K.Strasse "
			+ "HAVING ( ( SUM( A.Betrag ) > 49.99 ) ) "
			+ "ORDER BY Gesamt DESC";
	
	private String totalPerYear = "SELECT "
			+ " SUM( A.Betrag ) AS Gesamt, COUNT( A.ID_Auftrag ) AS Anz"
			+ " FROM Auftrag A "
			+ "WHERE A.Datum > {D '%s' } AND A.Datum < {D '%s' } ";

	@BeforeClass
	public static void setUpBefore() {
		dbs = new DBService();
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
			String sql = String.format(sqlAbove50, "2006-11-30","2007-12-01");
			ResultSet rs = dbs.execute(sql);
			int i = 0;
			while(rs.next()) {
				i++;
			}
			System.out.println("Insgesamt "+i+" Kandidaten über 50");
			
			sql = String.format(totalPerYear, "2006-11-30","2007-12-01");
			rs = dbs.execute(sql);
			while(rs.next()) {
				System.out.println("Umsatz: "+String.format("%.2f", rs.getFloat(1))+"\tAufträge: "+rs.getNString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void testExecute2008() {
		System.out.println("### 2008");
		try {
			String sql = String.format(sqlAbove50, "2007-11-30","2008-12-01");
			ResultSet rs = dbs.execute(sql);
			int i = 0;
			while(rs.next()) {
				i++;
			}
			System.out.println("Insgesamt "+i+" Kandidaten über 50");
			
			sql = String.format(totalPerYear, "2007-11-30","2008-12-01");
			rs = dbs.execute(sql);
			while(rs.next()) {
				System.out.println("Umsatz: "+String.format("%.2f", rs.getFloat(1))+"\tAufträge: "+rs.getNString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testExecute2009() {
		assertNotNull(dbs);
		String sql = String.format(sqlAbove50, "2008-11-30","2009-12-01");
		ResultSet rs = dbs.execute(sql);
		assertNotNull(rs);
		try {
			int i = 0;
			System.out.println("### 2009");
			while(rs.next()) {
				i++;
			}
			System.out.println("Insgesamt "+i+" Kandidaten über 50");
			
			sql = String.format(totalPerYear, "2008-11-30","2009-12-01");
			rs = dbs.execute(sql);
			while(rs.next()) {
				System.out.println("Umsatz: "+String.format("%.2f", rs.getFloat(1))+"\tAufträge: "+rs.getNString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testExecute2010() {
		assertNotNull(dbs);
		String sql = String.format(sqlAbove50, "2009-11-30","2010-12-01");
		ResultSet rs = dbs.execute(sql);
		assertNotNull(rs);
		try {
			int i = 0;
			System.out.println("### 2010");
			while(rs.next()) {
				i++;
//	        	System.out.println(i+"\t"+rs.getNString(3)+"\t"+rs.getNString(1)+"\t"+rs.getNString(2));
			}
			System.out.println("Insgesamt "+i+" Kandidaten über 50");
			
			sql = String.format(totalPerYear, "2009-11-30","2010-12-01");
			rs = dbs.execute(sql);
			while(rs.next()) {
				System.out.println("Umsatz: "+String.format("%.2f", rs.getFloat(1))+"\tAufträge: "+rs.getNString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testExecute2011() {
		assertNotNull(dbs);
		String sql = String.format(sqlAbove50, "2010-11-30","2011-12-01");
		ResultSet rs = dbs.execute(sql);
		assertNotNull(rs);
		try {
			int i = 0;
			System.out.println("### 2011");
	        while(rs.next()) {
	        	i++;
//	        	System.out.println(i+"\t"+rs.getNString(3)+"\t"+rs.getNString(1)+"\t"+rs.getNString(2));
	        }
	        System.out.println("Insgesamt "+i+" Kandidaten über 50");
	        
			sql = String.format(totalPerYear, "2010-11-30","2011-12-01");
			rs = dbs.execute(sql);
			while(rs.next()) {
				System.out.println("Umsatz: "+String.format("%.2f", rs.getFloat(1))+"\tAufträge: "+rs.getNString(2));
			}
        } catch (SQLException e) {
	        e.printStackTrace();
        }
	}
	
	@Test
	public void testExecute2012() {
		ResultSet rs = dbs.execute(String.format(sqlAbove50, "2011-11-30","2012-12-01"));
		assertNotNull(rs);
		try {
			int i = 0;
			System.out.println("### 2012");
	        while(rs.next()) {
	        	i++;
//	        	System.out.println(i+"\t"+rs.getNString(3)+"\t"+rs.getNString(1)+"\t"+rs.getNString(2));
	        }
	        System.out.println("Insgesamt "+i+" Kandidaten über 50");
	        
			rs = dbs.execute(String.format(totalPerYear, "2011-11-30","2012-12-01"));
			while(rs.next()) {
				System.out.println("Umsatz: "+String.format("%.2f", rs.getFloat(1))+"\tAufträge: "+rs.getNString(2));
			}
        } catch (SQLException e) {
	        e.printStackTrace();
        }
	}
	
	@Test
	public void testExecute2013() {
		ResultSet rs = dbs.execute(String.format(sqlAbove50, "2012-11-30","2013-12-01"));
		assertNotNull(rs);
		try {
			int i = 0;
			System.out.println("### 2013");
	        while(rs.next()) {
	        	i++;
//	        	System.out.println(i+"\t"+rs.getNString(3)+"\t"+rs.getNString(1)+"\t"+rs.getNString(2));
	        }
	        System.out.println("Insgesamt "+i+" Kandidaten über 50");
	        
			rs = dbs.execute(String.format(totalPerYear, "2012-11-30","2013-12-01"));
			while(rs.next()) {
				System.out.println("Umsatz: "+String.format("%.2f", rs.getFloat(1))+"\tAufträge: "+rs.getNString(2));
			}
        } catch (SQLException e) {
	        e.printStackTrace();
        }
	}
	
	@Test
	public void testExecute2014() {
		assertNotNull(dbs);
		ResultSet rs = dbs.execute(String.format(sqlAbove50,"2013-11-30","2014-12-01"));
		assertNotNull(rs);
		try {
			int i = 0;
			System.out.println("### 2014");
	        while(rs.next()) {
	        	i++;
//	        	System.out.println(i+"\t"+rs.getNString(3)+"\t"+rs.getNString(1)+"\t"+rs.getNString(2));
	        }
	        System.out.println("Insgesamt "+i+" Kandidaten über 50");
	        
			rs = dbs.execute(String.format(totalPerYear, "2013-11-30","2014-12-01"));
			while(rs.next()) {
				System.out.println("Umsatz: "+String.format("%.2f", rs.getFloat(1))+"\tAufträge: "+rs.getNString(2));
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
				+ " FROM Kunde K JOIN Auftrag A ON A.Kunde = K.ID_Kunde "
//				+ "{ OJ \"Kunden\" AS \"Kunden\" LEFT OUTER JOIN \"Karten\" AS \"Karten\" ON \"Kunden\".\"ID_Kunde\" = \"Karten\".\"Kunde\" }, "
//				+ " \"Auftrag\" AS \"Auftrag\", "
				+ "WHERE "
//				+ "AND \"Kunden\".\"Anrede_Typus\" = \"Kunden_Anreden\".\"ID_Anrede\" "
				+ " A.Datum > {D '2014-11-30' } "
				+ "AND A.Datum < {D '2015-12-01' } "
				+ "GROUP BY K.Nachname, K.Strasse "
//				+ "\"Kunden\".\"Strasse\", "
//				+ "\"Kunden\".\"Ort\", "
//				+ "\"Kunden\".\"PLZ\", "
//				+ "\"Karten\".\"wk_2012\", "
//				+ "\"Karten\".\"wk_2013\", \"Karten\".\"wk_2014\" "
				+ "HAVING ( ( SUM( A.Betrag ) > 49.99 ) ) "
				+ "ORDER BY Gesamt DESC";
		
		String sql2 = "SELECT Anrede, Nachname FROM Kunde "
				+ "select "
				+ "ID_KUNDE, "
				+ "SUM(Betrag) AS Umsatz "
				+ "FROM "
				+ " Kunde K JOIN Auftrag A ON K.ID_Kunde = A.Kunde "
				+ " WHERE A.Datum > '2014-11-30' AND A.Datum < '2015-12-01' "
				+ " GROUP BY K.ID_Kunde "
				+ " HAVING SUM(Betrag) > 49.99 "
				+ " order by Umsatz ASC";
		
		ResultSet rs = dbs.execute(sql);
//		ResultSet rs = dbs.execute("");
		assertNotNull(rs);
		try {
			int i = 0;
			System.out.println("### 2015");
	        while(rs.next()) {
	        	i++;
//	        	System.out.println(i+"\t"+rs.getNString(3)+"\t"+rs.getNString(1)+"\t"+rs.getNString(2));
	        }
	        System.out.println("Insgesamt "+i+" Kandidaten über 50");
	        
			sql = String.format(totalPerYear, "2014-11-30","2015-12-01");
			rs = dbs.execute(sql);
			while(rs.next()) {
				System.out.println("Umsatz: "+String.format("%.2f", rs.getFloat(1))+"\tAufträge: "+rs.getNString(2));
			}
        } catch (SQLException e) {
	        e.printStackTrace();
        }
	}
	
	@Test @Ignore
	public void testExecute2() {
		assertNotNull(dbs);
		ResultSet rs = dbs.execute("SELECT \"Kunde_Anrede\".\"Anrede_Bezeichnung\" AS \"Anrede\", "
				+ "\"Kunde\".\"Nachname\", "
				+ "\"Kunde\".\"Strasse\" AS \"Straße\", "
				+ "\"Kunde\".\"Ort\", "
				+ "\"Kunde\".\"PLZ\", "
				+ "SUM( \"Auftrag\".\"Betrag\" ) AS \"Gesamtbetrag\", "
				+ "COUNT( \"Auftrag\".\"ID_Auftrag\" ) AS \"Anzahl Aufträge\" "
//				+ "\"Karte\".\"wk_2003" AS "WK 03", 
//				"Karten"."wk_2004" AS "WK 04", 
//				"Karten"."wk_2005" AS "WK 05", 
//				"Karten"."wk_2006" AS "Wk 06", 
//				"Karten"."wk_2007" AS "WK 07" 
				+ "FROM "
//				+ "{ OJ \"Kunde\" LEFT OUTER JOIN \"Karte\" ON \"Kunde\".\"ID_Kunde\" = \"Karte\".\"Kunde\" }, "
				+ "Auftrag, Kunde_Anrede "
				+ "WHERE ( \"Auftrag\".\"Kunde\" = \"Kunde\".\"ID_Kunde\" AND \"Kunde\".\"Anrede_Typus\" = \"Kunde_Anrede\".\"ID_Anrede\" ) "
				+ "AND ( ( \"Auftrag\".\"Datum\" > {D '2006-11-06' } AND \"Auftrag\".\"Datum\" < {D '2007-12-01' } ) ) "
				+ "GROUP BY \"Kunde_Anrede\".\"Anrede_Bezeichnung\", "
				+ "\"Kunde\".\"Nachname\", \"Kunde\".\"Strasse\", \"Kunde\".\"Ort\", \"Kunde\".\"PLZ\" "
//				+ ""Karten"."wk_2003", "Karten"."wk_2004", "Karten"."wk_2005", "Karten"."wk_2006", "Karten"."wk_2007" "
				+ "HAVING ( ( SUM( \"Auftrag\".\"Betrag\" ) > 49.99 ) ) ORDER BY \"Kunde\".\"Nachname\" ASC");
		assertNotNull(rs);
		try {
	        while(rs.next()) {
	        	System.out.println(rs.getNString(1) + " "+rs.getNString(2));
	        }
        } catch (SQLException e) {
	        e.printStackTrace();
        }
	}
	
	@AfterClass
	public static void shutDown() throws SQLException {
		dbs.c.commit();
		dbs.c.close();
	}

}

