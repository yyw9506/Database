import java.sql.*;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;

public class CumulativeHistogram {
	
	private Connection conn = null;
	private PreparedStatement pstmt = null;
	
	public void startConnection(String dbname, String user, String password) {
		try {
			String url = "jdbc:db2://localhost:50000/" + dbname;
			Class.forName("com.ibm.db2.jcc.DB2Driver").newInstance();
			conn = DriverManager.getConnection(url, user, password);
			System.out.println("Database: " + dbname);
			System.out.println("Username: " + user);
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void stopConnection() {
		try {
			if (conn.isClosed() == false) {
				conn.close();
			}
			System.out.print("Connection closed.");
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void getCumulativeFrequency(double start, double end, int number) {
		
		List<Double> result_list = new ArrayList<Double>();
		
		try {
			// execute SQL
			String sql = "SELECT SALARY FROM EMPLOYEE";
			pstmt = conn.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			
			while(rs.next()) {
				String salary = rs.getString(1);
				result_list.add(Double.valueOf(salary));
			}
			
			System.out.println(result_list.size() + " records selected.");
			System.out.println("-----------------------------------------------------------");
			
			// defs 
			int[] frequency = new int[number];
			Arrays.fill(frequency, 0);
			
			int frequency_cum = 0;
			int frequency_index = 0;
			
			double interval = (end - start)/number;
			
			// count frequency
			for(Double salary: result_list) {
				if (salary >= start && salary < end) {
					frequency_index = (int)((salary - start)/interval);
					frequency[frequency_index] += 1;
				}
			}
			
			// display cumulative frequency
			System.out.println("binnum \t" + "cumulativefrequency \t" + "binstart \t" + "binend");
			for(int i=0; i<frequency.length; i++) {
				frequency_cum += frequency[i];
				String temp_start = String.format("%.2f", start);
				String temp_end = String.format("%.2f", (start + interval));
				System.out.println(i+1 + "\t\t" + frequency_cum + "\t\t" + temp_start + "        " + temp_end);
				start += interval;
			}
			System.out.println("-----------------------------------------------------------");
			
			// close result set
			rs.close();
			
			// close prepared statement
			pstmt.close();
			
			// commit
			conn.commit();
			
			// close connection
			conn.close();
			
		}catch(Exception e) {
			e.printStackTrace();
		}	
	}
	
	public static void main(String[] args) {
		
		double start = Double.valueOf(args[3]);
		double end = Double.valueOf(args[4]);
		int number = Integer.valueOf(args[5]);
		
		CumulativeHistogram ch = new CumulativeHistogram();
		
		ch.startConnection(args[0], args[1], args[2]);
		ch.getCumulativeFrequency(start, end, number);
		ch.stopConnection();
	}
}
