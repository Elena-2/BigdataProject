package bigdataman.burani.Gasem;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang.ArrayUtils;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.bson.Document;
import org.apache.hadoop.util.ToolRunner;

public class Gasem {
	
	private static String dbname;
	private static MongoClient client;
	private static MongoDatabase database;
	
	public static String getDBName() {
		return dbname;
	}
	
	public static void setDBName(String s) {
		dbname=s;
	}

	private static void initSystem()
	{
		LogManager.getLogger("org.mongodb.driver.connection").setLevel(Level.ERROR);
		LogManager.getLogger("org.mongodb.driver.management").setLevel(Level.ERROR);
		LogManager.getLogger("org.mongodb.driver.cluster").setLevel(Level.ERROR);
		LogManager.getLogger("org.mongodb.driver.protocol.insert").setLevel(Level.ERROR);
		LogManager.getLogger("org.mongodb.driver.protocol.query").setLevel(Level.ERROR);
		LogManager.getLogger("org.mongodb.driver.protocol.update").setLevel(Level.ERROR);
		try {
			client = new MongoClient("localhost", 27017);
			database = client.getDatabase(Gasem.getDBName());
			database.getCollection(Gasem.getDBName());
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}
	
	public static void main(String[] str){
		
		if (str.length != 1) {
			if(dbname==null) {
				throw new IllegalArgumentException("ENTER DATABASE NAME");
			}
		}

		Gasem.setDBName(str[0]);

		initSystem();
		System.out.println("DATABASE: " + dbname + "\n\rENTER COMMAND OR TYPE help FOR INSTRUCTIONS.");
		while(true) {
			BufferedReader in = null;
			try{
				in = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
				} catch (IOException localIOException) {}
			String line = null;
			try{
				line = in.readLine(); 
				} catch (IOException localIOException1) {}
			
			String[] input = line.split(" ");

			// MAPREDUCE
			if (input[0].equals("groupby")) {
				System.out.println("\n\rMAP-REDUCE\n\r");
				
				String args = "";
				String outputDoc = "";

				
				//SELECT key, COUNT(occurrences)
				if (input.length == 2) {
					args = input[1];
					outputDoc = args;
				}
				
				//SELECT key, COUNT(occurrences)/SUM/AVG WHERE <key=value> 
				else {
					
					//SELECT key, SUM WHERE <key=value>
					if(input[input.length-1].equals("sum")) {
						int a = 2;
						args =input[1];
						
						if(input[2].equals("or")) {
							args += "," +input[2];
							a++;
						}
						
						for(int i = a; i<input.length-1; i++) {
							if(input[i].split("=").length == 1)
								args += " "+input[i];
							else
								args += ","+input[i];
						}
						
						args += ","+input[input.length-1];
						
						outputDoc = input[input.length-1] +"_"+input[1];
						if(input.length>3) {
							outputDoc += "_WHERE";
						
							for(int k=2;k<input.length-1;k++) {
								if(input[2].equals("or"))
									outputDoc += "-or-" + input[k];
								else
									outputDoc += "_" + input[k];
							}
						}
					}
					
					//SELECT key, AVG WHERE <key=value>
					else if(input[input.length-1].equals("avg")) {
						int b = 2;
						args =input[1];
						
						if(input[2].equals("or")) {
							args += "," +input[2];
							b++;
						}
						
						for(int i = b; i<input.length-1; i++) {
							if(input[i].split("=").length == 1)
								args += " "+input[i];
							else
								args += ","+input[i];
						}
						
						args += ","+input[input.length-1];
						
						outputDoc = input[input.length-1] +"_"+input[1];
						if(input.length>3) {
							outputDoc += "_WHERE";
							for(int k=2;k<input.length-1;k++) {
								if(input[2].equals("or"))
									outputDoc += "-or-" + input[k];
								else
									outputDoc += "_" + input[k];
							}
						}
					}
					
					//SELECT key, COUNT(occurrences) WHERE <key=value>
					else {
						int c = 2;
						args=input[1];
						
						if(input[2].equals("or")) {
							args += "," +input[2];
							c++;
						}
						
						for(int i = c; i<input.length; i++) {
							if(input[i].split("=").length == 1)
								args += " "+input[i];
							else
								args += ","+input[i];
						}

						outputDoc = input[1]+"_WHERE";
						for(int k=c;k<input.length;k++) {
							if(input[2].equals("or"))
								outputDoc += "-or-" + input[k];
							else
								outputDoc += "_" + input[k];
						}
					}
				}


				String result = "";
				if(outputDoc.split("=").length>1)
					outputDoc = outputDoc.split("=")[0]+"_"+outputDoc.split("=")[1];
				result = outputDoc; 
				
				try {
					ToolRunner.run(new Tools(args, result), input);
				}
				catch (Exception localException) {
					System.out.println("ERROR");
				}
				
				MongoCollection<Document> outputCollection = database.getCollection("MapRed_" + result);
				
				InputOutput.outputAggregate(outputCollection, "MapRed_" + result);

				InputOutput.updatelog("MapRed_", (String[])ArrayUtils.subarray(input, 1, input.length));

			}
			
			//LOG
			else if (input[0].equals("log")) {
				InputOutput.getLog();

			}
			
			// HELP
			else if (input[0].equals("help")) {
				System.out.println("\n\r"
						+ "\n\rCOMMAND SYNTAX:\n\r "
						+ " \n\r "
						+ "MAP-REDUCE: \n\r "
						+ "groupby key \n\r "
						+ "groupby key sum_mwe\n\r "
						+ "groupby key avg_mwe\n\r "
						+ "groupby key key=value \n\r "
						+ "groupby key key=value sum_mwe \n\r "
						+ "groupby key key=value avg_mwe \n\r "
						+ "groupby key key1=value1 ... keyN=valueN \n\r "
						+ "groupby key key1=value1 ... keyN=valueN sum_mwe\n\r "
						+ "groupby key key1=value1 ... keyN=valueN avg_mwe\n\r "
						+ "groupby key or key1=value1 key1=value2... key1=valueN \n\r "
						+ "groupby key or key1=value1 key1=value2... key1=valueN sum_mwe\n\r "
						+ "groupby key or key1=value1 key1=value2... key1=valueN avg_mwe\n\r "
						+ "THE SAME OPERATIONS CAN ALSO BE DONE BY SPECIFYING TWO AGGREGATION KEYS WITH THE SYNTAX:\n\r "
						+ "groupby key1-key2 ... \n\r "
						+ " \n\r "
						+ "LOG OPERATION: \n\r "
						+ "log \n\r"
						+ " \n\r "
						+ "EXIT:\n\r "
						+ "exit");
			}

			//EXIT
			else if (input[0].equals("exit")) {
				break;

			}
			
			//INPUT-ERROR
			else
			{
				System.out.println("INCORRECT COMMAND -> TYPE help FOR INFORMATION");
			}
			System.out.println();
			System.out.println("\n\r OPERATION COMPLETED\n\r");

		}
	}

}