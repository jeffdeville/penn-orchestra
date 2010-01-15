/*
 * Copyright (C) 2010 Trustees of the University of Pennsylvania
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS of ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.upenn.cis.orchestra;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Calendar;


public class Test {

//	public static /*final*/ String DRIVER = "COM.ibm.db2.jdbc.app.DB2Driver";
	protected static final String DRIVER = Config.getJDBCDriver();
	protected static String SERVER = "jdbc:db2://localhost:50000/BULK2";

	
    public static void compareDeleteDrop(Connection con, int tuples) throws Exception{
        // create and execute a SELECT
        PreparedStatement stmt = con.prepareStatement("create table foo (x int not null primary key, y int, z int, u int, v int, w int) not logged initially");
        int num = stmt.executeUpdate();
        
        PreparedStatement stmtI = con.prepareStatement("INSERT INTO foo values (?, ?, ?, ?, ?, ?)");
        for(int i = 0; i < tuples; i++){
        	stmtI.setInt(1, i);
        	stmtI.setInt(2, i);
        	stmtI.setInt(3, i);
        	stmtI.setInt(4, i);
        	stmtI.setInt(5, i);
        	stmtI.setInt(6, i);
			
        	num = stmtI.executeUpdate();
	    }
        

        PreparedStatement stmtD1 = con.prepareStatement("DELETE from foo");
        Calendar before = Calendar.getInstance();
        num = stmtD1.executeUpdate();
        Calendar after = Calendar.getInstance();
        
        long time = after.getTimeInMillis() - before.getTimeInMillis();
        System.out.println("DELETE: " + num + " in " + time + "msec");
        for(int i = 0; i < tuples; i++){
        	stmtI.setInt(1, i);
        	num = stmtI.executeUpdate();
	    }
        
        PreparedStatement stmtD2 = con.prepareStatement("DROP TABLE foo");
        before = Calendar.getInstance();
        num = stmtD2.executeUpdate();
        num = stmt.executeUpdate();
        after = Calendar.getInstance();
        time = after.getTimeInMillis() - before.getTimeInMillis();
        System.out.println("DROP/CREATE: " + time + "msec");
        
        stmt.close();
        stmtI.close();
        stmtD1.close();
        stmtD2.close();
        
        PreparedStatement drop = con.prepareStatement("drop table foo");
        drop.executeUpdate();
        drop.close();
    }
    public static void compareRenameCopy(Connection con, int tuples) throws Exception{
        // create and execute a SELECT
        PreparedStatement stmt = con.prepareStatement("create table foo (x int not null primary key, y int) not logged initially");
        int num = stmt.executeUpdate();
        PreparedStatement stmt2 = con.prepareStatement("create table bar (x int not null primary key, y int) not logged initially");
        num = stmt2.executeUpdate();
        
        PreparedStatement stmtI = con.prepareStatement("INSERT INTO foo values (?, ?)");
        for(int i = 0; i < tuples; i++){
        	stmtI.setInt(1, i);
        	stmtI.setInt(2, i);
        	num = stmtI.executeUpdate();
	    }
        
        stmtI = con.prepareStatement("INSERT INTO bar values (?, ?)");
        for(int i = 0; i < tuples; i++){
        	stmtI.setInt(1, i);
        	stmtI.setInt(2, i);
        	num = stmtI.executeUpdate();
	    }
        
        
        PreparedStatement stmt3 = con.prepareStatement("DELETE from bar");
        PreparedStatement stmt4 = con.prepareStatement("INSERT INTO bar (select * from foo F where not exists (select * from bar B where F.x = B.x and F.y = B.y))");
        PreparedStatement stmt5 = con.prepareStatement("DELETE from foo");
        Calendar before = Calendar.getInstance();
        num = stmt3.executeUpdate();
        num = stmt4.executeUpdate();
        num = stmt5.executeUpdate();
        Calendar after = Calendar.getInstance();
        
        long time = after.getTimeInMillis() - before.getTimeInMillis();
        System.out.println("COPY: " + time + "msec");
        stmt3 = con.prepareStatement("DELETE from foo");
        stmt4 = con.prepareStatement("INSERT INTO foo (select * from bar F)");
        stmt5 = con.prepareStatement("DELETE from bar");
        before = Calendar.getInstance();
        num = stmt3.executeUpdate();
        num = stmt4.executeUpdate();
        num = stmt5.executeUpdate();
        after = Calendar.getInstance();
        
        time = after.getTimeInMillis() - before.getTimeInMillis();
        System.out.println("COPY2: " + time + "msec");
        
        PreparedStatement stmt6 = con.prepareStatement("DROP TABLE bar");
        PreparedStatement stmt7 = con.prepareStatement("RENAME TABLE foo to bar");
        
        before = Calendar.getInstance();
        num = stmt6.executeUpdate();
        num = stmt7.executeUpdate();
        num = stmt.executeUpdate();
        after = Calendar.getInstance();
        time = after.getTimeInMillis() - before.getTimeInMillis();
        System.out.println("RENAME: " + time + "msec");
        
        stmt.close();
        stmtI.close();
        stmt2.close();
        stmt3.close();
        stmt4.close();
        stmt5.close();
        stmt6.close();
        stmt7.close();
        
        PreparedStatement drop = con.prepareStatement("drop table foo");
        drop.executeUpdate();
        drop = con.prepareStatement("drop table bar");
        drop.executeUpdate();
        drop.close();
    }
    
    public static void bulkLoad(Connection con){
    	try{
    		con.setAutoCommit(false);
    		
    		File f = new File("C:\\path\\to\\file");
    		BufferedReader input = null;
    	    input = new BufferedReader( new FileReader(f) );
    	    String line = null;
    	    
    	    PreparedStatement stmt = con.prepareStatement("insert into BULKTEST VALUES(?, ?, ?, ?, ?, ?)");
    	    
    	    while (( line = input.readLine()) != null){
    	    	String [] attrs = line.split("\\|");
    	    	
    	    	stmt.setInt(1, Integer.parseInt(attrs[0]));
    	    	for(int i = 1; i < 6; i++){
    	    		stmt.setString(i+1, attrs[i]);
    	    	}
    	    	System.out.println("QUERY WITH PARAMS: " + stmt);
    	    	stmt.addBatch();

//    	    	int num = stmt.executeUpdate();
//        	    System.out.println("IMPORT RETURNED: " + num + " tuples");
    	    }
    	    
            Calendar before = Calendar.getInstance();
            int ret[] = stmt.executeBatch();
            Calendar after = Calendar.getInstance();

            long importTime = after.getTimeInMillis() - before.getTimeInMillis();
            int num = 0;
            for(int j = 0; j < ret.length; j++){
            	num += ret[j];
            }
            
    	    System.out.println("DATA IMPORT TIME: " + importTime + " msec");
            before = Calendar.getInstance();
            con.commit();
            after = Calendar.getInstance();
            long commitTime = after.getTimeInMillis() - before.getTimeInMillis();
            long totalTime = importTime + commitTime;
            
            System.out.println("COMMIT TIME: " + commitTime + " msec");
            System.out.println("TOTAL TIME: " + totalTime + " msec");
            
    	    System.out.println("IMPORT RETURNED: " + num + " tuples");
    	    stmt.close();


    	    PreparedStatement copy = con.prepareStatement("INSERT INTO BULKTESTCOPY (select * from BULKTEST)");
            before = Calendar.getInstance();
            int copiedTuples = copy.executeUpdate();
            after = Calendar.getInstance();

            long copyTime = after.getTimeInMillis() - before.getTimeInMillis();
            
    	    before = Calendar.getInstance();
            con.commit();
            after = Calendar.getInstance();
            commitTime = after.getTimeInMillis() - before.getTimeInMillis();
            
            System.out.println("COPY TIME: " + copyTime + " msec");
            System.out.println("COMMIT TIME: " + commitTime + " msec");
            
    	    System.out.println("COPY RETURNED: " + copiedTuples + " tuples");
    	    copy.close();
    	    Statement clear = con.createStatement();
    	    clear.executeUpdate("DELETE FROM BULKTEST");
    	    clear.executeUpdate("DELETE FROM BULKTESTCOPY");
    	    con.commit();
    	    clear.close();

    	}catch(Exception e){
    		e.printStackTrace();
    	}
    }
    
    public static void conOverhead(Connection con, int tuples) throws Exception{
        // create and execute a SELECT
        PreparedStatement stmt = con.prepareStatement("create table foo (x int not null, y int) not logged initially");
        int num = stmt.executeUpdate();
        stmt = con.prepareStatement("create index ind on foo(x)");
        
        Statement stmtDD1 = con.createStatement();
//        stmtDD1.execute("RUNSTATS ON TABLE foo ON ALL COLUMNS ALLOW WRITE ACCESS");
        stmtDD1.execute("IMPORT FROM \"C:\\path\\to\\file OF DEL MODIFIED BY COLDEL| METHOD P (1, 2) MESSAGES NUL INSERT INTO foo(x,y)");
        

        //PreparedStatement stmtI = con.prepareStatement("INSERT INTO foo values (?, ?)");
        PreparedStatement stmtI = con.prepareStatement("INSERT INTO foo values (1, 2)");
        Calendar before = Calendar.getInstance();
        for(int i = 0; i < tuples; i++){
        	//stmtI.setInt(1, i);
        	//stmtI.setInt(2, i);
        	num = stmtI.executeUpdate();
	    }

        Calendar after = Calendar.getInstance();
        long time = after.getTimeInMillis() - before.getTimeInMillis();
        System.out.println("OVERHEAD FOR " + tuples + ": " + time + "msec");
        
        stmt.close();
        stmtI.close();
                
        PreparedStatement drop = con.prepareStatement("drop table foo");
        drop.executeUpdate();
        
        drop.close();
    }
    
	public static void main(String argv[]) throws Exception{
		
        // Load the JDBC-ODBC bridge
        Class.forName (DRIVER);

        // specify the ODBC data source's URL
        Connection con = DriverManager.getConnection(SERVER,"username","password");
        //con.setAutoCommit(false);
        
	    try {
	    	//System.out.println("RENAME vs COPY for 10-10000 tuples");
	    	//compareRenameCopy(con, 10);
	    	//compareRenameCopy(con, 100);
	    	//compareRenameCopy(con, 1000);
	    	//compareRenameCopy(con, 5000);
	    	//compareRenameCopy(con, 10000);
	    	
	    	//System.out.println("DELETE vs DROP for 10-50000 tuples");
	    	//compareDeleteDrop(con, 10);
	    	//compareDeleteDrop(con, 100);
	    	//compareDeleteDrop(con, 1000);
	    	//compareDeleteDrop(con, 10000);
	    	//compareDeleteDrop(con, 50000);
	    	//compareDeleteDrop(con, 100000);
	    	//compareDeleteDrop(con, 500000);
	    	
//	    	conOverhead(con, 10000);
//	    	conOverhead(con, 100000);

	    	for(int i = 0; i < 5; i++){
	    		bulkLoad(con);
	    	}
	    	
	    	/*
	        
	        // traverse through results
	        System.out.println("Found row:");

	        while (rs.next()) {
	        	// get current row values
	        	String Surname = rs.getString(1);
	        	String FirstName = rs.getString(2);
	        	int Category = rs.getInt(3);

	        	// print values
	        	System.out.print (" Surname=" + Surname);
	        	System.out.print (" FirstName=" + FirstName);
	        	System.out.print (" Category=" + Category);
	        	System.out.print(" \n");
	        }*/

	        // close statement and connection
	        //con.commit();
	        con.close();
	    } catch (java.lang.Exception ex) {
	        ex.printStackTrace();
	        PreparedStatement drop = con.prepareStatement("drop table foo");
	        drop.executeUpdate();
	        drop.close();
	        con.close(); 
        }
    }

}

