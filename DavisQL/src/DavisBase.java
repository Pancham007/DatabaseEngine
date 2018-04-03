import java.io.RandomAccessFile;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class DavisBase {

	
	static String prompt = "panchsql> ";
	static String version = "v1.0";
	static String copyright = "©Pancham Mamania";
	static boolean isExit = false;
		
	public static int pageSize = 512;
	
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	static Scanner ans=new Scanner(System.in);
	/** ***********************************************************************
	 *  Main method
	 */
	
    public static void main(String[] args) {
    	init();
		
		splashScreen();

		System.out.print("Would you like to customize the prompt name? (Y/N) : ");
		/*
		String answer =ans.next().trim().toLowerCase();
		if(answer.equals("y"))
		{
			System.out.print("Please enter the name of the prompt of your choice :  ");
			prompt=ans.next().trim();
			prompt=prompt+">";
		}
		*/
		String userCommand = ""; 

		while(!isExit) {
			System.out.print(prompt);
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");

	}
    
    public static void init(){
		try {
			File database_Dir = new File("data");
			if(database_Dir.mkdir()){
				System.out.println("The data base doesn't exit, initializing data base...");
				initialize();
			}
			else {
				
				String[] oldFiles = database_Dir.list();
				boolean checkTable = false;
				boolean checkColumn = false;
				for (int i=0; i<oldFiles.length; i++) {
					if(oldFiles[i].equals("davisbase_tables.tbl"))
						checkTable = true;
					if(oldFiles[i].equals("davisbase_columns.tbl"))
						checkColumn = true;
				}
				
				if(!checkTable){
					System.out.println("The davisbase_tables does not exit, initializing data base...");
					System.out.println();
					initialize();
				}
				
				if(!checkColumn){
					System.out.println("The davisbase_columns table does not exit, initializing data base...");
					System.out.println();
					initialize();
				}
				
			}
		}
		catch (SecurityException e) {
			System.out.println(e);
		}

	}
	
public static void initialize() {
		
	try {
		File database_Dir = new File("data");
		database_Dir.mkdir();
		String[] oldFiles;
		oldFiles = database_Dir.list();
		for (int i=0; i<oldFiles.length; i++) {
			File aFile = new File(database_Dir, oldFiles[i]); 
			aFile.delete();
		}
	}
	catch (SecurityException e) {
		System.out.println(e);
	}
		try {
		RandomAccessFile catalog_table = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
		catalog_table.setLength(pageSize);
		catalog_table.seek(0);
		catalog_table.write(0x0D);
		catalog_table.writeByte(0x02);
		
		int offsetTable=pageSize-24;
		int offsetColumn=offsetTable-25;
			
		catalog_table.writeShort(offsetColumn);
		catalog_table.writeInt(0);
		catalog_table.writeInt(0);
		catalog_table.writeShort(offsetTable);
		catalog_table.writeShort(offsetColumn);
			
		catalog_table.seek(offsetTable);
		catalog_table.writeShort(20);
		catalog_table.writeInt(1); 
		catalog_table.writeByte(1);
		catalog_table.writeByte(28);
		catalog_table.writeBytes("davisbase_tables");
			
		catalog_table.seek(offsetColumn);
		catalog_table.writeShort(21);
		catalog_table.writeInt(2); 
		catalog_table.writeByte(1);
		catalog_table.writeByte(29);
		catalog_table.writeBytes("davisbase_columns");
			
		catalog_table.close();
	}
	catch (Exception e) {
		System.out.println(e);
	}
	
	try {
		RandomAccessFile catalog_columns = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
		catalog_columns.setLength(pageSize);
		catalog_columns.seek(0);       
		catalog_columns.writeByte(0x0D); 
		catalog_columns.writeByte(0x08); 
			
		int[] offset=new int[10];
		offset[0]=pageSize-43;
		offset[1]=offset[0]-47;
		offset[2]=offset[1]-44;
		offset[3]=offset[2]-48;
		offset[4]=offset[3]-49;
		offset[5]=offset[4]-47;
		offset[6]=offset[5]-57;
		offset[7]=offset[6]-49;
			
		catalog_columns.writeShort(offset[7]); 
		catalog_columns.writeInt(0); 
		catalog_columns.writeInt(0); 
		
		for(int i=0;i<8;i++)
			catalog_columns.writeShort(offset[i]);

			
		catalog_columns.seek(offset[0]);
		catalog_columns.writeShort(33);
		catalog_columns.writeInt(1); 
		catalog_columns.writeByte(5);
		catalog_columns.writeByte(28);
		catalog_columns.writeByte(17);
		catalog_columns.writeByte(15);
		catalog_columns.writeByte(4);
		catalog_columns.writeByte(14);
		catalog_columns.writeBytes("davisbase_tables"); 
		catalog_columns.writeBytes("rowid"); 
		catalog_columns.writeBytes("INT"); 
		catalog_columns.writeByte(1); 
		catalog_columns.writeBytes("NO"); 	
			
		catalog_columns.seek(offset[1]);
		catalog_columns.writeShort(39); 
		catalog_columns.writeInt(2); 
		catalog_columns.writeByte(5);
		catalog_columns.writeByte(28);
		catalog_columns.writeByte(22);
		catalog_columns.writeByte(16);
		catalog_columns.writeByte(4);
		catalog_columns.writeByte(14);
		catalog_columns.writeBytes("davisbase_tables"); 
		catalog_columns.writeBytes("table_name"); 
		catalog_columns.writeBytes("TEXT"); 
		catalog_columns.writeByte(2);
		catalog_columns.writeBytes("NO"); 
		
		catalog_columns.seek(offset[2]);
		catalog_columns.writeShort(34); 
		catalog_columns.writeInt(3); 
		catalog_columns.writeByte(5);
		catalog_columns.writeByte(29);
		catalog_columns.writeByte(17);
		catalog_columns.writeByte(15);
		catalog_columns.writeByte(4);
		catalog_columns.writeByte(14);
		catalog_columns.writeBytes("davisbase_columns");
		catalog_columns.writeBytes("rowid");
		catalog_columns.writeBytes("INT");
		catalog_columns.writeByte(1);
		catalog_columns.writeBytes("NO");
			
		catalog_columns.seek(offset[3]);
		catalog_columns.writeShort(40);
		catalog_columns.writeInt(4); 
		catalog_columns.writeByte(5);
		catalog_columns.writeByte(29);
		catalog_columns.writeByte(22);
		catalog_columns.writeByte(16);
		catalog_columns.writeByte(4);
		catalog_columns.writeByte(14);
		catalog_columns.writeBytes("davisbase_columns");
		catalog_columns.writeBytes("table_name");
		catalog_columns.writeBytes("TEXT");
		catalog_columns.writeByte(2);
		catalog_columns.writeBytes("NO");
		
		catalog_columns.seek(offset[4]);
		catalog_columns.writeShort(41);
		catalog_columns.writeInt(5); 
		catalog_columns.writeByte(5);
		catalog_columns.writeByte(29);
		catalog_columns.writeByte(23);
		catalog_columns.writeByte(16);
		catalog_columns.writeByte(4);
		catalog_columns.writeByte(14);
		catalog_columns.writeBytes("davisbase_columns");
		catalog_columns.writeBytes("column_name");
		catalog_columns.writeBytes("TEXT");
		catalog_columns.writeByte(3);
		catalog_columns.writeBytes("NO");
			
		catalog_columns.seek(offset[5]);
		catalog_columns.writeShort(39);
		catalog_columns.writeInt(6); 
		catalog_columns.writeByte(5);
		catalog_columns.writeByte(29);
		catalog_columns.writeByte(21);
		catalog_columns.writeByte(16);
		catalog_columns.writeByte(4);
		catalog_columns.writeByte(14);
		catalog_columns.writeBytes("davisbase_columns");
		catalog_columns.writeBytes("data_type");
		catalog_columns.writeBytes("TEXT");
		catalog_columns.writeByte(4);
		catalog_columns.writeBytes("NO");
		
		catalog_columns.seek(offset[6]);
		catalog_columns.writeShort(49); 
		catalog_columns.writeInt(7); 
		catalog_columns.writeByte(5);
		catalog_columns.writeByte(29);
		catalog_columns.writeByte(28);
		catalog_columns.writeByte(19);
		catalog_columns.writeByte(4);
		catalog_columns.writeByte(14);
		catalog_columns.writeBytes("davisbase_columns");
		catalog_columns.writeBytes("ordinal_position");
		catalog_columns.writeBytes("TINYINT");
		catalog_columns.writeByte(5);
		catalog_columns.writeBytes("NO");
		
		catalog_columns.seek(offset[7]);
		catalog_columns.writeShort(41); 
		catalog_columns.writeInt(8); 
		catalog_columns.writeByte(5);
		catalog_columns.writeByte(29);
		catalog_columns.writeByte(23);
		catalog_columns.writeByte(16);
		catalog_columns.writeByte(4);
		catalog_columns.writeByte(14);
		catalog_columns.writeBytes("davisbase_columns");
		catalog_columns.writeBytes("is_nullable");
		catalog_columns.writeBytes("TEXT");
		catalog_columns.writeByte(6);
		catalog_columns.writeBytes("NO");
		
		catalog_columns.close();
	}
	catch (Exception e) {
	System.out.println(e);
	}
}

	
/**
 *  Display the splash screen
 */
	public static void splashScreen() {
	System.out.println(line("-",80));
    System.out.println("Welcome to DavisBaseLite"); // Display the string.
	System.out.println("DavisBaseLite Version " + getVersion());
	System.out.println(getCopyright());
	System.out.println("\nType \"help;\" to display supported commands.");
	System.out.println(line("-",80));
}

/**
 * @param s The String to be repeated
 * @param num The number of time to repeat String s.
 * @return String A String object, which is the String s appended to itself num times.
 */
public static String line(String s,int num) {
	String a = "";
	for(int i=0;i<num;i++) {
		a += s;
	}
	return a;
}

	/**
	 *  Help: Display supported commands
	 */
	public static void help() {
		System.out.println(line("*",150));
		System.out.println("SUPPORTED COMMANDS");
		System.out.println("All commands below are case insensitive");
		System.out.println();
		System.out.println("\tSHOW TABLES;											Display all tables in the database.");
		System.out.println("\tCREATE TABLE table_name (<column_name> <data_type> [NOT NULL]);					Create a new table with the specified columns in the database");
		System.out.println("\tDROP TABLE table_name;										Remove table data and its schema.");
		System.out.println("\tINSERT INTO table_name [column_list] VALUES value_list;						Inserts a single record into a table");
		System.out.println("\tDELETE FROM TABLE table_name WHERE row_id= key_value;						Deletes a single row/record from a table whose row_id is <key_value> (Unique value)");
		System.out.println("\tUPDATE table_name SET column_name = value [WHERE condition]					Modifies one or more records in a table");
		System.out.println("\tSELECT * FROM table_name;                        						Display all records in the table.");
		System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;  						Display records whose rowid is <id>.");
		System.out.println("\tVERSION;                                         						Show the program version.");
		System.out.println("\tHELP;                                            						Show this help information");
		System.out.println("\tEXIT;                                            						Exit the program");
		System.out.println();
		System.out.println();
		System.out.println(line("*",150));
	}

	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}
	
	public static String getCopyright() {
		return copyright;
	}
	
	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}

	public static void parseUserCommand (String userCommand) {
		
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		switch (commandTokens.get(0)) {

		    case "show":
			    showTables();
			    break;

		    case "select":
				parseQueryString(userCommand);
				break;

		    case "drop":
				dropTable(userCommand);
				break;	
		    
		    case "create":
				parseCreateString(userCommand);
			    break;

			case "insert":
				parseInsertString(userCommand);
				break;
				
			case "delete":
				parseDeleteString(userCommand);
				break;	

			case "update":
				parseUpdateString(userCommand);
				break;
			
			case "help":
				help();
				break;

			case "version":
				displayVersion();
				break;

			case "exit":
				isExit=true;
				break;
				
			case "quit":
				isExit=true;
				break;
	
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");

				break;
		}
	} 

	public static boolean isTableExists(String table){
		table = table+".tbl";
		
		try {
			File database_Dir = new File("data");
			String[] oldFiles;
			oldFiles = database_Dir.list();
			for (int i=0; i<oldFiles.length; i++) {
				if(oldFiles[i].equals(table))
					return true;
			}
		}
		catch (SecurityException e) {
			System.out.println("Unable to create data container directory");
			System.out.println(e);
		}

		return false;
	}

	public static String[] parserEquation(String equation)
		{
		String temp[] = new String[2];
		String comparator[] = new String[3];
		if(equation.contains("=")) {
			temp = equation.split("=");
			comparator[0] = temp[0].trim();
			comparator[1] = "=";
			comparator[2] = temp[1].trim();
		}

		if(equation.contains("<")) {
			temp = equation.split("<");
			comparator[0] = temp[0].trim();
			comparator[1] = "<";
			comparator[2] = temp[1].trim();
		}
		
		if(equation.contains(">")) {
			temp = equation.split(">");
			comparator[0] = temp[0].trim();
			comparator[1] = ">";
			comparator[2] = temp[1].trim();
		}
		
		if(equation.contains("<=")) {
			temp = equation.split("<=");
			comparator[0] = temp[0].trim();
			comparator[1] = "<=";
			comparator[2] = temp[1].trim();
		}

		if(equation.contains(">=")) {
			temp = equation.split(">=");
			comparator[0] = temp[0].trim();
			comparator[1] = ">=";
			comparator[2] = temp[1].trim();
		}

		return comparator;
	}
		

	public static void showTables() {
		System.out.println("STUB: Calling the method to process the command");
		System.out.println("Parsing the string:\"show tables\"");
		
		String table = "davisbase_tables";
		String[] columns = {"table_name"};
		String[] compute = new String[0];
		Table.select(table, columns, compute);
	}
	
    public static void parseCreateString(String queryString) {
		
		System.out.println("STUB: Calling your method to process the command");
		System.out.println("Parsing the string:\"" + queryString + "\"");
		
		String[] tokens=queryString.split(" ");
		String tableName = tokens[2];
		String[] temp = queryString.split(tableName);
		String columns = temp[1].trim();
		String[] create_columns = columns.substring(1, columns.length()-1).split(",");
		
		for(int i = 0; i < create_columns.length; i++)
			{
			create_columns[i] = create_columns[i].trim();
			System.out.println(create_columns[i]);
			}
		if(isTableExists(tableName)){
			System.out.println("Table "+tableName+" already exists.");
		}
		else
			{
			Table.createTable(tableName, create_columns);		
			}

	}
    
    public static void parseInsertString(String queryString) {
		System.out.println("STUB: Calling the method to process the command");
		System.out.println("Parsing the string:\"" + queryString + "\"");
		
		String[] tokens=queryString.split(" ");
		String table = tokens[2];
		String[] temp = queryString.split("values");
		String temporary=temp[1].trim();
		String[] insert_values = temporary.substring(1, temporary.length()-1).split(",");
		for(int i = 0; i < insert_values.length; i++)
			insert_values[i] = insert_values[i].trim();
		if(!isTableExists(table)){
			System.out.println("Table "+table+" does not exist.");
		}
		else
		{
			Table.insert_into(table, insert_values);
		}

	}
    
    public static void parseDeleteString(String queryString) {
		System.out.println("STUB: Calling the method to process the command");
		System.out.println("Parsing the string:\"" + queryString + "\"");
		
		String[] tokens=queryString.split(" ");
		String table = tokens[3];
		String[] temp = queryString.split("where");
		String expTemp = temp[1];
		String[] exp = parserEquation(expTemp);
		if(!isTableExists(table)){
			System.out.println("Table "+table+" does not exist.");
		}
		else
		{
			Table.delete(table, exp);
		}
		
		
	}
    
    public static void parseUpdateString(String queryString) {
		System.out.println("STUB: Calling the method to process the command");
		System.out.println("Parsing the string:\"" + queryString + "\"");
		
		String[] tokens=queryString.split(" ");
		String table = tokens[1];
		String[] temp1 = queryString.split("set");
		String[] temp2 = temp1[1].split("where");
		String expTemp = temp2[1];
		String setTemp = temp2[0];
		String[] exp = parserEquation(expTemp);
		String[] set = parserEquation(setTemp);
		if(!isTableExists(table)){
			System.out.println("Table "+table+" does not exist.");
		}
		else
		{
			Table.update(table, exp, set);
		}
		
	}
    
    public static void parseQueryString(String queryString) {
		System.out.println("STUB: Calling the method to process the command");
		System.out.println("Parsing the string:\"" + queryString + "\"");
		
		String[] cmp;
		String[] column;
		String[] temp = queryString.split("where");
		if(temp.length > 1){
			String tmp = temp[1].trim();
			cmp = parserEquation(tmp);
		}
		else{
			cmp = new String[0];
		}
		String[] select = temp[0].split("from");
		String table_name = select[1].trim();
		String cols = select[0].replace("select", "").trim();
		if(cols.contains("*")){
			column = new String[1];
			column[0] = "*";
		}
		else{
			column = cols.split(",");
			for(int i = 0; i < column.length; i++)
				column[i] = column[i].trim();
		}
		
		if(!isTableExists(table_name)){
			System.out.println("Table "+table_name+" does not exist.");
		}
		else
		{
		    Table.select(table_name, column, cmp);
		}
	}
	
	public static void dropTable(String dropTableString) {
		System.out.println("STUB: Calling the method to process the command");
		System.out.println("Parsing the string:\"" + dropTableString + "\"");
		
		String[] tokens=dropTableString.split(" ");
		String table_name = tokens[2];
		if(!isTableExists(table_name)){
			System.out.println("Table "+table_name+" does not exist.");
		}
		else
		{
			Table.drop(table_name);
		}		

	}
		

}