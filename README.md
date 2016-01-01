# maven-hsqldb-csv

## Description

This is an example how to integrate CSV-Files as Data-Sources to an Application using embedded [HSQLDB] 
as Storage System. A System like this can be used to exchange an [OpenOffice Base] Datamanagement by a thin and costumized Client-Application. 
All Necessary Dependencies and Test Resources are managed by [Apache Maven], the Implementation itself
is done with plain, Oldschool Java-SQL. They come together with some Integration Tests by [JUnit] to 
illustrate the Workflow.

## Build and Test Instructions
You'll need Apache Maven 3.0+ and JDK 1.7+. 
Clone the repository locally and run afterwards in your Terminal:
```bash 
cd maven-hsqldb-csv
mvn clean test
```
This will execute the Tests immediatly. Testdata represents a simple Customer - Order - Sales Scenario,
where a bunch of Customers launches Orders with an Amount and Date. Some of the Testcases query for the Total Sum of Orders from a Customer per Year and
announce when a certain High Sales Condition has been met. Since the Data includes only 2 Customers and 3 Orders for 3 Years,
there's not much to report, but feel free to extend this example.

## HSQLDB
[HSQLDB] offers the Feature, to treat CSV or other delimited files like SQL tables without importing those explicitly into a running
external Database-Server (like other DBMS like MySQL can do, too). The [HSQLDB]-Way of doing this is called "Text Tables" (see corresponding [HSQLDB Doc] for more Informations).
Handling CSV-Files as Datasources for a [HSQLDB]-Application can be tricky. The Crux is, that the Database starts to create several hidden Files in the Folder
where the local Database is supposed to reside. Don't edit these unless you really know what you're doing!
The Definition of the Database-Schema with the Connection of CSV to Database can be found inside the "schema.ql"-File at src/main/resources/data/.
Please note, that you usually only need to execute the Schema-Files once when you first start your local embedded DB-App and HSQLDB will remember the Connections
as you may note afterwards in the hidden File .script.

## Licence
All Code placed in the public domain without any Warranty.

[Apache Maven]:https://maven.apache.org/
[HSQLDB]:http://hsqldb.org/
[HSQLDB Doc]:http://hsqldb.org/doc/2.0/guide/texttables-chapt.html
[JUnit]:http://junit.org/
