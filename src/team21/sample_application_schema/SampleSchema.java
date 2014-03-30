package team21.sample_application_schema;

import java.sql.*;

import simpledb.remote.SimpleDriver;

/**
 * Sample Application Schema to test out SimpleDB with SQl Queries
 * 
 * @author Team 21
 *
 */

public class SampleSchema {
	public static void main(String[] args){
		
		Connection connection = null;

		try {  
			// Open connection with database
			Driver driver = new SimpleDriver();
			String host = "localhost";
			String url = "jdbc:simpledb://" + host;
		
			connection = driver.connect(url, null); // Get connection
			
			Statement statement = connection.createStatement(); // Get statement
			
			/////////////////////// Drop Tables if Exist //////////////////////////////////////////
			
			//TODO Drop table if exist to avoid filling them after every run
			// Interestingly, trying to create a table that already exists doesn't throw an error
			
			/////////////////////// Create and Fill Tables ////////////////////////////////////////
			
			// First table - Soccer teams table
			String query = "create table SoccerTeams(ID int, Name varchar(40), Country " +
							"varchar(40), Colors varchar(30), League varchar(20))";
			statement.executeUpdate(query); // Create soccer teams table
			
			// Second table - Baseball teams table
			query = "create table BaseballTeams(ID int, Name varchar(40), City " +
					"varchar(40), Colors varchar(30))";
			statement.executeUpdate(query); // Create baseball teams table
			
			// Populate soccer teams table
			query = "insert into SoccerTeams(ID, Name, Country, Colors, League) values ";
			String[] soccerTeams = {"(1, 'Juventus', 'Italy', 'White and Black', 'Serie A')",
									"(2, 'Roma', 'Italy', 'Red', 'Serie A')",
									"(3, 'Chievo', 'Italy', 'Red', 'Serie B')",
									"(4, 'Real Madrid', 'Spain', 'White', 'BBVA')",
									"(5, 'Barcelona', 'Spain', 'Red and Blue', 'BBVA')",
									"(6, 'PSG', 'France', 'Blue', 'Ligue One')",
									"(7, 'Chelsea', 'England', 'Blue', 'Barclays')"};

			// Loop through soccer teams array to fill table
			for (int i = 0; i < soccerTeams.length; i++){
				statement.executeUpdate(query + soccerTeams[i]);
			}
			
			// Populate baseball teams table
			query = "insert into BaseballTeams(ID, Name, City, Colors) values ";
			String[] baseballTeams = {"(1, 'Red Sox', 'Boston', 'Red')",
									"(2, 'White Sox', 'Chicago', 'White')",
									"(3, 'Cubs', 'Chicago', 'Blue')",
									"(4, 'Tigers', 'Detroit', 'White')",
									"(5, 'Marlins', 'Miami', 'Black')",
									"(6, 'Yankees', 'New York', 'Grey')",
									"(7, 'Giants', 'San Francisco', 'Grey')"};

			// Loop through baseball teams array to fill table
			for (int i = 0; i < baseballTeams.length; i++){
				statement.executeUpdate(query + baseballTeams[i]);
			}
			
			/////////////////////// Query Tables and Print Results/////////////////////////////////
			
			ResultSet results;
			
			// First query - All soccer teams in table
			query = "select Name, Country, Colors, League " +
					"from SoccerTeams";
			results = statement.executeQuery(query);
			
			// Print results
			System.out.printf("%-15s \t %-10s \t %-25s \t %-10s\n\n", "Team", "Country",
					"Colors", "League");
			while (results.next()) {
				System.out.printf("%-15s \t %-10s \t %-25s \t %-10s\n", 
						results.getString("Name"),
						results.getString("Country"),
						results.getString("Colors"),
						results.getString("League"));
			}
			results.close();
			
			//TODO Add more queries, some of them with WHERE clauses
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (connection != null){
					connection.close();
				}
			} catch (SQLException e){
				e.printStackTrace();
			}
		}
	}
}

