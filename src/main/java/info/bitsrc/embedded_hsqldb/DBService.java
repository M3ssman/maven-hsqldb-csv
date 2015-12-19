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
public class DBService {

	Connection c;
	Statement s;
	
	public DBService() {
		this("/data/","/data/schema.sql");
	}
	
	public DBService(String dbPath, String schemaPath) {
		initDatabase(dbPath);
		String[] sqlCreateSchemas = extractCreateSchemaSQL(schemaPath);
        execute(sqlCreateSchemas);
	}

	void execute(String[] sqls) {
		for(String s : sqls) {
			if(s.trim().length() > 1 && ! s.startsWith("--"))
				execute(s);
		}
	}
	
	ResultSet execute(String sqlCreateSchema) {
	    try {
        	Statement stm = c.createStatement();
	        stm.execute(sqlCreateSchema);
	        ResultSet rs = stm.getResultSet(); 
	        return rs;
        } catch (SQLException e) {
	        e.printStackTrace();
        }
	    return null;
    }

	String[] extractCreateSchemaSQL(String path) {
	    URL u =  getClass().getResource(path);
	    File f = new File(u.getFile());
	    List<String> ls = new ArrayList<>();
	    try (Scanner scanner = new Scanner(f)) {
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				ls.add(line);
			}
			scanner.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	    return ls.toArray(new String[ls.size()]);
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
