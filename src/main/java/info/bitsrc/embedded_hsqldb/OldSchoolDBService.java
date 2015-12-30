package info.bitsrc.embedded_hsqldb;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * 
 * Establish Connection to Local DB Files from CSV Data Sets
 * 
 * @author u.hartwig
 *
 */
public class OldSchoolDBService {

	private Connection c;

	public OldSchoolDBService() {
		this("/data/", "/data/schema.sql");
	}

	public OldSchoolDBService(String dbPath, String schemaPath) {
		initDatabase(dbPath);
		String[] sqlCreateSchemas = extractCreateSchemaSQL(schemaPath);
		if (sqlCreateSchemas.length > 0) {
			execute(sqlCreateSchemas);
		}
	}

	void execute(String[] sqls) {
		for (String s : sqls) {
			execute(s);
		}
	}

	ResultSet execute(String sqlCreateSchema) {
		try (Statement s = c.createStatement()) {
			s.execute(sqlCreateSchema);
			ResultSet rs = s.getResultSet();
			return rs;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	int insert(String sqlString) {
		try (Statement s = c.createStatement()) {
			int key = s.executeUpdate(sqlString, Statement.RETURN_GENERATED_KEYS);
			return key;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return 0;
	}

	String[] extractCreateSchemaSQL(String path) {
		URL u = getClass().getResource(path);
		File f = new File(u.getFile());
		List<String> ls = new ArrayList<>();
		try (Scanner scanner = new Scanner(f)) {
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				if (isImportantLine(line))
					ls.add(line);
			}
			scanner.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ls.toArray(new String[ls.size()]);
	}

	boolean isImportantLine(String line) {
		return line.trim().length() > 1 && !line.startsWith("--");
	}

	void initDatabase(String dbPath) {
		String driver = "org.hsqldb.jdbcDriver";
		try {
			Class.forName(driver);
			System.out.println("Driver was successfully loaded.");
			String path = getClass().getResource(dbPath).getPath();
			c = DriverManager.getConnection("jdbc:hsqldb:file:" + path, "sa", "");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
