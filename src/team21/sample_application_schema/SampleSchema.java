package team21.sample_application_schema;

import java.sql.*;

import simpledb.remote.SimpleDriver;

/**
 * CS4432-Project1: Sample Application Schema to test out SimpleDB with SQl Queries
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
			
			/////////////////////// Create and Fill Tables ////////////////////////////////////////
			
			// First table - Soccer teams table
			String query = "create table SoccerTeams(ID int, Name varchar(40), Country " +
							"varchar(40), Colors varchar(30), League varchar(20))";
			statement.executeUpdate(query); // Create soccer teams table
			
			// Second table - Baseball teams table
			query = "create table BaseballTeams(ID int, Name varchar(40), City " +
					"varchar(40), Colors varchar(30))";
			statement.executeUpdate(query); // Create baseball teams table
			
			// Third table - Computers table
			query = "create table Computers(ID int, Brand varchar(40), Color " +
					"varchar(40), Price int)";
			statement.executeUpdate(query); // Create computer table
			
			// Fourth table - Cars table
			query = "create table Cars(ID int, Name varchar(40), Make " +
					"varchar(40), Color varchar(30))";
			statement.executeUpdate(query); // Create cars table
			
			// Fifth table - Colors table
			query = "create table Colors(ID int, Name varchar(40), HEX " +
					"varchar(40),  Liked varchar(4))";
			statement.executeUpdate(query); // Create colors teams table
			
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
			
			// Populate computers table
			query = "insert into Computers(ID, Brand, Color, Price) values ";
			String[] computers = {"(1, 'Dell', 'Black', 700)",
									"(2, 'Apple', 'White', 2800)",
									"(3, 'HP', 'Blue', 850)",
									"(4, 'Asus', 'Black', 1200)",
									"(5, 'Alienware', 'Black', 1850)"};

			// Loop through soccer teams array to fill table
			for (int i = 0; i < computers.length; i++){
				statement.executeUpdate(query + computers[i]);
			}
			
			// Populate cars table
			query = "insert into Cars(ID, Name, Make, Color) values ";
			String[] cars = {"(1, 'Camaro', 'Chevrolet', 'Yellow')",
									"(2, 'Tacoma', 'Toyota', 'Blue')",
									"(3, 'Evoque', 'Land Rover', 'White')",
									"(4, 'Carrera', 'Porsche', 'Red')",
									"(5, 'XF', 'Jaguar', 'Sand')"};

			// Loop through soccer teams array to fill table
			for (int i = 0; i < cars.length; i++){
				statement.executeUpdate(query + cars[i]);
			}
			
			// Populate cars table
			query = "insert into Colors(ID, Name, HEX, Liked) values ";
			String[] colors = {"(1, 'Blue', '00F', 'Yes')",
									"(2, 'Green', '0F0', 'No')",
									"(3, 'Red', 'F00', 'No')",
									"(4, 'Yellow', 'FF0', 'Yes')",
									"(5, 'White', 'FFF', 'Yes')"};

			// Loop through soccer teams array to fill table
			for (int i = 0; i < colors.length; i++){
				statement.executeUpdate(query + colors[i]);
			}
			
			/////////////////////// Query Tables and Print Results/////////////////////////////////
			
			ResultSet results;
			
			// First query - All soccer teams in table
			query = "select Name, Country, Colors, League " +
					"from SoccerTeams";
			results = statement.executeQuery(query);
			
			// Print results
			for(int i = 60; i >= 0; i--){System.out.print("-");}
			System.out.println("\nAll soccer teams in table");
			System.out.printf("\n%-15s \t %-10s \t %-25s \t %-10s\n\n", "Team", "Country",
					"Colors", "League");
			while (results.next()) {
				System.out.printf("%-15s \t %-10s \t %-25s \t %-10s\n", 
						results.getString("Name"),
						results.getString("Country"),
						results.getString("Colors"),
						results.getString("League"));
			}
			for(int i = 60; i >= 0; i--){System.out.print("-");}
			results.close();
			
			// Second query - Update information on Soccer teams and print
			// Update value
			query = "update SoccerTeams set Colors='Bianco e Nero' where Name='Juventus'";
			statement.executeUpdate(query);
			
			// Query updated tuple
			query = "select Name, Country, Colors, League from SoccerTeams where Name='Juventus'";
			results = statement.executeQuery(query);
			
			// Print result
			System.out.println("\nSoccer team with modified colors");
			System.out.printf("\n%-15s \t %-10s \t %-25s \t %-10s\n\n", "Team", "Country",
					"Colors", "League");
			while (results.next()) {
				System.out.printf("%-15s \t %-10s \t %-25s \t %-10s\n", 
						results.getString("Name"),
						results.getString("Country"),
						results.getString("Colors"),
						results.getString("League"));
			}
			for(int i = 60; i >= 0; i--){System.out.print("-");}
			results.close();
			
			// Third query - Print specific tuples
			query = "select Name from BaseballTeams where Colors='White'";
			results = statement.executeQuery(query);
			
			// Print results
			System.out.printf("\n%-15s\n\n", "Baseball teams with white uniforms");
			while (results.next()) {
				System.out.printf("%-15s\n", results.getString("Name"));
			}
			for(int i = 60; i >= 0; i--){System.out.print("-");}
			results.close();
			
			// Fourth query - All computers in table
			query = "select Brand, Color, Price " +
					"from Computers";
			results = statement.executeQuery(query);
			
			// Print results
			System.out.println("\nAll computers in table");
			System.out.printf("\n%-15s \t %-10s \t %-25s\n\n", "Brand", "Color",
					"Price");
			while (results.next()) {
				System.out.printf("%-15s \t %-10s \t %-25d\n", 
						results.getString("Brand"),
						results.getString("Color"),
						results.getInt("Price"));
			}
			for(int i = 60; i >= 0; i--){System.out.print("-");}
			results.close();
			
			// Fifth query - All cars in table
			query = "select Name, Make, Color " +
					"from Cars";
			results = statement.executeQuery(query);
			
			// Print results
			System.out.println("\nAll cars in table");
			System.out.printf("\n%-15s \t %-10s \t %-25s\n\n", "Name", "Make",
					"Color");
			while (results.next()) {
				System.out.printf("%-15s \t %-10s \t %-25s\n", 
						results.getString("Name"),
						results.getString("Make"),
						results.getString("Color"));
			}
			for(int i = 60; i >= 0; i--){System.out.print("-");}
			results.close();
			
			// Sixth query - All colors in table			
			query = "select Name, HEX, Liked " +
					"from Colors";
			results = statement.executeQuery(query);
			
			// Print results
			System.out.println("\nAll colors in table");
			System.out.printf("\n%-15s \t %-10s \t %-25s\n\n", "Name", "HEX",
					"Liked");
			while (results.next()) {
				System.out.printf("%-15s \t %-10s \t %-25s\n", 
						results.getString("Name"),
						results.getString("HEX"),
						results.getString("Liked"));
			}
			for(int i = 60; i >= 0; i--){System.out.print("-");}
			results.close();
			
			// Seventh query - All soccer teams in table
			query = "select Name, Country, Colors, League " +
					"from SoccerTeams";
			results = statement.executeQuery(query);
			
			// Print results
			System.out.println("\nAll soccer teams in table");
			System.out.printf("\n%-15s \t %-10s \t %-25s \t %-10s\n\n", "Team", "Country",
					"Colors", "League");
			while (results.next()) {
				System.out.printf("%-15s \t %-10s \t %-25s \t %-10s\n", 
						results.getString("Name"),
						results.getString("Country"),
						results.getString("Colors"),
						results.getString("League"));
			}
			for(int i = 60; i >= 0; i--){System.out.print("-");}
			results.close();
			
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

