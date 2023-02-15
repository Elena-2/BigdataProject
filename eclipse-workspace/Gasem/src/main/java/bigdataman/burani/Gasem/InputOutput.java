package bigdataman.burani.Gasem;

import java.io.IOException;
import com.mongodb.Block;
import com.mongodb.client.MongoCollection;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.io.FileOutputStream;
import org.bson.Document;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.util.Date;

public class InputOutput{
	
	private static BufferedWriter outFile = null;
	private static BufferedWriter logFile = null;

	private static Block<Document> printBlock = new Block<Document>() {
		
		public void apply(Document document) {
			try {
				InputOutput.outFile.newLine();
				InputOutput.outFile.write(document.toJson());
				InputOutput.outFile.flush();
			} catch (Exception e) { System.out.println("OUTPUT ERROR");}
			
		}
	};

	public static void outputAggregate(MongoCollection<Document> outputCollection, String name) {
		try {
			File f = new File("output/" + name + ".json");
			if(f.exists() && !f.isDirectory()) { 
				System.out.println("COLLECTION ALREADY EXISTING");
			} else {
				outFile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("output/" + name + ".json", true), "UTF-8"));
				outputCollection.find().forEach(printBlock);
			}
		} catch (IOException e) {
			System.out.println("WRITING ERROR");
		} 
	}

	public static void updatelog(String operation, String[] param){
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		
		try
		{
			File log = new File("output/log.txt");
			boolean exists = true;
			if (!log.exists())
				exists = false;
			logFile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("output/log.txt", true), "UTF-8"));
			if (!exists)
				logFile.write(dateFormat.format(date) + "  \nLOG FILE - DOCUMENT DATABASE  "+Gasem.getDBName().toUpperCase()+"\n" + dateFormat.format(date) + "\n\r\n\r");
			logFile.newLine();
			logFile.write(dateFormat.format(date) + " -> " + operation + ", PARAMETERS: " + Arrays.toString(param));	
			logFile.flush();
		} catch (IOException e) {
			System.out.println("LOG ERROR");
		}
	}

	public static void getLog(){
		
		BufferedReader inFile = null;
		try
		{
			inFile = new BufferedReader(new InputStreamReader(new FileInputStream("output/log.txt")));
			String s; 
			while ((s = inFile.readLine()) != null) { 
			System.out.println(s);
			}
		} catch (IOException e) { System.out.println("File log error");
		}
	}
}