package info.bitsrc.embedded_hsqldb;

import static org.junit.Assert.assertEquals;
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
 * Integration Tests for {@link OldSchoolDBService} with letting Apache Maven handle the Test-Resources.
 * 
 * @author u.hartwig
 *
 */
public class OldSchoolDBServiceTest {

	private String fileSchemaDefinitions = "/data/schema.sql";
	private String fileCSVCustomer = "/data/customer.csv";

	static OldSchoolDBService dbs;
	static final float THRESHOLD = 49.99f;

	private String findCustomersWithHighSales = ""
	        + "SELECT Name, Street, SUM( Amount ) AS Total, COUNT( O.ID ) AS Nr "
	        + "FROM HSQLDB_EMBEDDED.CUSTOMER C JOIN HSQLDB_EMBEDDED.ORDER O ON C.ID = O.ID_Customer "
	        + "WHERE "
	        + " Order_Date > {D '%s' } AND Order_Date < {D '%s' } "
	        + "GROUP BY Name, Street, City "
	        + "HAVING ( ( SUM( Amount ) > " + THRESHOLD + " ) ) "
	        + "ORDER BY Total DESC";

	private String salesAndOrdersPerYear = "SELECT "
	        + "SUM( O.Amount ) AS Total, COUNT( O.ID ) AS Nr "
	        + "FROM HSQLDB_EMBEDDED.ORDER O "
	        + "WHERE Order_Date > {D '%s' } AND Order_Date < {D '%s' } ";

	@BeforeClass
	public static void setUpBefore() {
		dbs = new OldSchoolDBService();
		assertNotNull(dbs);
	}

	/**
	 * Here we look whether we've succeeded to load one of the CSF-Files that hold our Data.
	 * We only check one of the three Files, since they get loaded by Folder-Name.
	 */
	@Test
	public void testExistenceFile1() {
		assertNotNull("Test file '" + fileCSVCustomer + "' missing", getClass().getResource(fileCSVCustomer));
	}

	/**
	 * The Schema Definitions are stored inside the main/resources-Folder.
	 * We need explicitly tell Maven to take care of this Folder, too,
	 * by adding it as a Test-Resource in the build-Section of the POM-File.
	 */
	@Test
	public void testExistenceFile2() {
		assertNotNull("Test file '" + fileSchemaDefinitions + "' missing", getClass()
		        .getResource(fileSchemaDefinitions));
	}

	/**
	 * 
	 * Since we had a Customer back in 2013 that committed 2 Orders, we expect also 1 Customer with a Order Sum
	 * above the defined {@link #THRESHOLD}.
	 * 
	 * See the order.csv-File for more Details.
	 * 
	 * @throws SQLException
	 */
	@Test
	public void testHighSales2013() throws SQLException {
		assumeNotNull(dbs);
		int actual = calculateNrAboveThreshold(String.format(findCustomersWithHighSales, "2012-11-30", "2013-12-01"));
		assertTrue(actual == 1);
		System.out.println(actual + " Customers had orders with sum >= 50");
	}
	
	@Test
	public void testSumAndOrders2013() throws SQLException {
		assumeNotNull(dbs);
		ResultSet rs = dbs.execute(String.format(salesAndOrdersPerYear, "2012-11-30", "2013-12-01"));
		assertNotNull(rs);
		while (rs.next()) {
			float actualSum = rs.getFloat(1);
			int actualOrders = rs.getInt(2);
			assertTrue(actualSum > 0);
			assertEquals(actualOrders, 2);
			printResults(actualSum, actualOrders);
		}
	}

	/**
	 * 
	 * No High Sales in 2014
	 * 
	 * @throws SQLException
	 */
	@Test
	public void testHighSales2014() throws SQLException {
		assumeNotNull(dbs);
		int actual = calculateNrAboveThreshold(String.format(findCustomersWithHighSales, "2013-11-30", "2014-12-01"));
		assertTrue(actual == 0);
		System.out.println(actual + " Customers had orders with sum >= 50");
	}

	/**
	 * 
	 * No Orders in 2014, so we expect nothing in the ResultSet.
	 * 
	 * @throws SQLException
	 */
	@Test
	public void testSumAndOrders2014() throws SQLException {
		assumeNotNull(dbs);
		ResultSet rs = dbs.execute(String.format(salesAndOrdersPerYear, "2013-11-30", "2014-12-01"));
		assertNotNull(rs);
		while (rs.next()) {
			float actualSum = rs.getFloat(1);
			int actualOrders = rs.getInt(2);
			assertTrue(actualSum == 0);
			assertEquals(actualOrders, 0);
			printResults(actualSum, actualOrders);
		}
	}

	/**
	 * 
	 * One Order in 2015, but below the {@link #THRESHOLD}, so we have no Customer above Threshold.
	 * 
	 * @throws SQLException
	 */
	@Test
	public void testHighSales2015() throws SQLException {
		assumeNotNull(dbs);
		int actual = calculateNrAboveThreshold(String.format(findCustomersWithHighSales, "2014-11-30", "2015-12-01"));
		assertTrue(actual == 0);
		System.out.println(actual + " Customers had orders with sum >= 50");
	}

	/**
	 * 
	 * We expect 1 Order and a Sum > 0
	 * 
	 * @throws SQLException
	 */
	@Test
	public void testSumAndOrders2015() throws SQLException {
		assumeNotNull(dbs);
		ResultSet rs = dbs.execute(String.format(salesAndOrdersPerYear, "2014-11-30", "2015-12-01"));
		assertNotNull(rs);
		while (rs.next()) {
			float actualSum = rs.getFloat(1);
			int actualOrders = rs.getInt(2);
			assertTrue(actualSum > 0);
			assertEquals(actualOrders, 1);
			printResults(actualSum, actualOrders);
		}
	}

	/**
	 * 
	 * We insert a new Customer and receive it's new ID-Key.
	 * The new Key must, of course, differ from the two existing IDs.
	 * Since this is only done at Test Stage, the Insertion doesn't persist,
	 * but you might spot it when you observe the test-classes-Folder in
	 * the Target Section right after the Test.
	 * 
	 * Please note, that these Artifacts will be removed when you do a "mvn clean".
	 * 
	 */
	@Test
	public void testInsertCUSTOMER() {
		String sqlInsert = "INSERT INTO HSQLDB_EMBEDDED.CUSTOMER (ID, Title, Name, Street, City, ZIP) VALUES (DEFAULT, 'Mr', 'Musterman', 'Rübenstreet 88','Mustercity','00100');";
		for (int i = 0; i < 1; i++) {
			int rs = dbs.insert(sqlInsert);
			assertNotEquals(0, rs);
		}
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

	void printResults(float actualSum, int actualOrders) {
	    String sumSales = String.format("%.2f", actualSum);
	    System.out.println("Sum 'o Sales: " + sumSales + "\tOrders: " + String.valueOf(actualOrders));
	}

	@AfterClass
	public static void shutDown() throws SQLException {
		dbs.c.commit();
		dbs.c.close();
	}

}
