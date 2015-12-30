package info.bitsrc.embedded_hsqldb;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * Short Integration Tests for {@link OldSchoolDBService}.
 * 
 * @author u.hartwig
 *
 */
public class OldSchoolDBServiceTest {

	private String fileSchemaDefinitions = "/data/schema.sql";
	private String fileCUSTOMERCSV = "/data/customer.csv";

	static OldSchoolDBService dbs;

	private String findCUSTOMERsWithHighSales = ""
	        + "SELECT Name, Street, SUM( Amount ) AS Total, COUNT( O.ID ) AS Nr "
	        + "FROM HSQLDB_EMBEDDED.CUSTOMER C JOIN HSQLDB_EMBEDDED.ORDERS O ON C.ID = O.ID_Customer "
	        + "WHERE "
	        + " Order_Date > {D '%s' } AND Order_Date < {D '%s' } "
	        + "GROUP BY Name, Street, City "
	        + "HAVING ( ( SUM( Amount ) > 49.99 ) ) "
	        + "ORDER BY Total DESC";

	private String salesAndOrdersPerYear = "SELECT "
	        + "SUM( O.Amount ) AS Total, COUNT( O.ID ) AS Nr "
	        + "FROM HSQLDB_EMBEDDED.ORDERS O "
	        + "WHERE Order_Date > {D '%s' } AND Order_Date < {D '%s' } ";

	@BeforeClass
	public static void setUpBefore() {
		dbs = new OldSchoolDBService();
		assertNotNull(dbs);
	}

	@Test
	public void testExistenceFile1() {
		assertNotNull("Test file '" + fileCUSTOMERCSV + "' missing", getClass().getResource(fileCUSTOMERCSV));
	}

	@Test
	public void testExistenceFile2() {
		assertNotNull("Test file '" + fileSchemaDefinitions + "' missing", getClass()
		        .getResource(fileSchemaDefinitions));
	}

	@Test
	public void testExecute2013() throws SQLException {
		assumeNotNull(dbs);
		int actual = calculateNrAboveThreshold(String.format(findCUSTOMERsWithHighSales, "2012-11-30", "2013-12-01"));
		assertTrue(actual == 1);
		System.out.println(actual + " Customers had orders with sum >= 50");
		calculateSalesAndOrders(String.format(salesAndOrdersPerYear, "2012-11-30", "2013-12-01"));
	}

	@Test
	public void testExecute2014() throws SQLException {
		assumeNotNull(dbs);
		int actual = calculateNrAboveThreshold(String.format(findCUSTOMERsWithHighSales, "2013-11-30", "2014-12-01"));
		assertTrue(actual == 0);
		System.out.println(actual + " Customers had orders with sum >= 50");
		calculateSalesAndOrders(String.format(salesAndOrdersPerYear, "2013-11-30", "2014-12-01"));
	}

	@Test
	public void testExecute2015() throws SQLException {
		assumeNotNull(dbs);
		int actual = calculateNrAboveThreshold(String.format(findCUSTOMERsWithHighSales, "2014-11-30", "2015-12-01"));
		assertTrue(actual == 0);
		System.out.println(actual + " Customers had orders with sum >= 50");
		calculateSalesAndOrders(String.format(salesAndOrdersPerYear, "2014-11-30", "2015-12-01"));
	}

	@Test
	public void testInsertCUSTOMER() {
		String sqlInsert = "INSERT INTO HSQLDB_EMBEDDED.CUSTOMER (ID, Title, Name, Street, City, ZIP) VALUES (DEFAULT, 'Mr', 'Musterman', 'Rübenstreet 88','Mustercity','00100');";
		for (int i = 0; i < 1; i++) {
			int rs = dbs.insert(sqlInsert);
			assertNotEquals(0, rs);
		}
	}

	@AfterClass
	public static void shutDown() throws SQLException {
		dbs.c.commit();
		dbs.c.close();
	}

	int calculateNrAboveThreshold(String sql) throws SQLException {
		ResultSet rs = dbs.execute(sql);
		assertNotNull(rs);
		int i = 0;
		while (rs.next()) {
			i++;
		}
		return i;
	}

	void calculateSalesAndOrders(String sql) throws SQLException {
		ResultSet rs = dbs.execute(sql);
		while (rs.next()) {
			System.out.println("Sales: " + String.format("%.2f", rs.getFloat(1)) + "\tOrders: "
			        + rs.getNString(2));
		}
	}

}
