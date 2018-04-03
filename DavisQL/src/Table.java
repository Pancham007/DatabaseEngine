import java.io.RandomAccessFile;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Table{
	
	public static int page_size = 512;
	public static String date_pattern = "yyyy-MM-dd_HH:mm:ss";

	public static void main(String[] args){}
//pages	
	public static int num_pages(RandomAccessFile file){
		int num_of_pages = 0;
		try{
			num_of_pages = (int)(file.length()/(new Long(page_size)));
		}catch(Exception e){
			System.out.println(e);
		}

		return num_of_pages;
	}
//retrieveValues	
	public static String[] retrieve_data(RandomAccessFile file, long location)
	{
		
		String[] values = null;
		try{
			
			SimpleDateFormat date_format = new SimpleDateFormat (date_pattern);

			file.seek(location+2);
			int row_id = file.readInt();
			int num_columns = file.readByte();
			
			byte[] type_of_data = new byte[num_columns];
			file.read(type_of_data);
			
			values = new String[num_columns+1];
			
			values[0] = Integer.toString(row_id);
			
			for(int i=1; i <= num_columns; i++){
				switch(type_of_data[i-1]){
					case 0x00:  file.readByte();
					            values[i] = "null";
								break;

					case 0x01:  file.readShort();
					            values[i] = "null";
								break;

					case 0x02:  file.readInt();
					            values[i] = "null";
								break;

					case 0x03:  file.readLong();
					            values[i] = "null";
								break;

					case 0x04:  values[i] = Integer.toString(file.readByte());
								break;

					case 0x05:  values[i] = Integer.toString(file.readShort());
								break;

					case 0x06:  values[i] = Integer.toString(file.readInt());
								break;

					case 0x07:  values[i] = Long.toString(file.readLong());
								break;

					case 0x08:  values[i] = String.valueOf(file.readFloat());
								break;

					case 0x09:  values[i] = String.valueOf(file.readDouble());
								break;

					case 0x0A:  Long temp_date = file.readLong();
								Date date_time = new Date(temp_date);
								values[i] = date_format.format(date_time);
								break;

					case 0x0B:  temp_date = file.readLong();
								Date date = new Date(temp_date);
								values[i] = date_format.format(date).substring(0,10);
								break;

					default:    int len = new Integer(type_of_data[i-1]-0x0C);
								byte[] bytes = new byte[len];
								file.read(bytes);
								values[i] = new String(bytes);
								break;
				}
			}

		}catch(Exception e){
			System.out.println(e);
		}

		return values;
	}


	public static int calculate_payload(String table, String[] values, byte[] type_of_data)
	{
		String[] dataType = get_data_type(table);
		int size =dataType.length;
		for(int i = 1; i < dataType.length; i++){
			type_of_data[i - 1]= get_type_of_data(values[i], dataType[i]);
			size = size + field_length(type_of_data[i - 1]);
		}
		return size;
	}
	
//getStc
	public static byte get_type_of_data(String value, String data_type)
	{
		if(value.equals("null")){
			switch(data_type){
				case "TINYINT":     return 0x00;
				case "SMALLINT":    return 0x01;
				case "INT":			return 0x02;
				case "BIGINT":      return 0x03;
				case "REAL":        return 0x02;
				case "DOUBLE":      return 0x03;
				case "DATETIME":    return 0x03;
				case "DATE":        return 0x03;
				case "TEXT":        return 0x03;
				default:			return 0x00;
			}							
		}else{
			switch(data_type){
				case "TINYINT":     return 0x04;
				case "SMALLINT":    return 0x05;
				case "INT":			return 0x06;
				case "BIGINT":      return 0x07;
				case "REAL":        return 0x08;
				case "DOUBLE":      return 0x09;
				case "DATETIME":    return 0x0A;
				case "DATE":        return 0x0B;
				case "TEXT":        return (byte)(value.length()+0x0C);
				default:			return 0x00;
			}
		}
	}
	

    public static short field_length(byte type_of_data)
    {
		switch(type_of_data){
			case 0x00: return 1;
			case 0x01: return 2;
			case 0x02: return 4;
			case 0x03: return 8;
			case 0x04: return 1;
			case 0x05: return 2;
			case 0x06: return 4;
			case 0x07: return 8;
			case 0x08: return 4;
			case 0x09: return 8;
			case 0x0A: return 8;
			case 0x0B: return 8;
			default:   return (short)(type_of_data - 0x0C);
		}
	}


//searchKeyPage	
    public static int searchPageWithKey(RandomAccessFile file, int key)
    {
		int value = 1;
		try{
			int num_of_pages = num_pages(file);
			for(int page = 1; page <= num_of_pages; page++){
				file.seek((page - 1)*page_size);
				byte page_type = file.readByte();
				if(page_type == 0x0D){
					int[] keys = Page.get_key_array(file, page);
					if(keys.length == 0)
						return 0;
					int right_most = Page.get_right_most(file, page);
					if(keys[0] <= key && key <= keys[keys.length - 1]){
						return page;
					}else if(right_most == 0 && keys[keys.length - 1] < key){
						return page;
					}
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}

		return value;
	}


//get_data_type	
	public static String[] get_data_type(String table)
	{
		String[] data_type = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] exp = {"table_name","=",table};
			filter(file, exp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> arr = new ArrayList<String>();
			for(String[] v : content.values()){
				arr.add(v[3]);
			}
			int size=arr.size();
			data_type = arr.toArray(new String[size]);
			file.close();
			return data_type;
		}catch(Exception e){
			System.out.println(e);
		}
		return data_type;
	}
//getColName
	public static String[] getColumnName(String table)
	{
		String[] columns = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] exp = {"table_name","=",table};
			filter(file, exp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[2]);
			}
			int size=array.size();
			columns = array.toArray(new String[size]);
			file.close();
			return columns;
		}catch(Exception e){
			System.out.println(e);
		}
		return columns;
	}
//getNullable
	public static String[] get_nullable(String table)
	{
		String[] nullable = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] exp = {"table_name","=",table};
			filter(file, exp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> arr = new ArrayList<String>();
			for(String[] v : content.values()){
				arr.add(v[5]);
			}
			int size=arr.size();
			nullable = arr.toArray(new String[size]);
			file.close();
			return nullable;
		}catch(Exception e){
			System.out.println(e);
		}
		return nullable;
	}

//cmpCheck
	public static boolean expCheck(String[] values, int rowid, String[] exp, String[] column_name)
	{

		boolean check = false;
			
		if(exp.length == 0){
			check = true;
		}
		else{
			int column_position = 1;
			for(int i = 0; i < column_name.length; i++){
				if(column_name[i].equals(exp[0])){
					column_position = i + 1;
					break;
				}
			}
			
			if(column_position == 1){
				int value = Integer.parseInt(exp[2]);
				String operator = exp[1];
				switch(operator){
					case "=": if(rowid == value) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">": if(rowid > value) 
								check = true;
							  else
								check = false;
						  	break;
					case ">=": if(rowid >= value) 
						        check = true;
					          else
					        	  check = false;	
				          	break;
					case "<": if(rowid < value) 
								check = true;
						  	else
						  		check = false;
						  	break;
					case "<=": if(rowid <= value) 
								check = true;
  						  	else
  						  		check = false;	
						  	break;
					case "!=": if(rowid != value)  
								check = true;
						  	else
						  		check = false;	
						  	break;						  							  							  							
					}
				}else{
					if(exp[2].equals(values[column_position-1]))
						check = true;
					else
						check = false;
			}
		}
		return check;
	}

	public static void filter(RandomAccessFile file, String[] exp, String[] column_name, Buffer buffer)
	{
		try{
			
			int num_of_pages = num_pages(file);
			for(int page = 1; page <= num_of_pages; page++){
				
				file.seek((page-1)*page_size);
				byte page_type = file.readByte();
				if(page_type == 0x0D)
				{
					byte num_of_cells = Page.get_cell_number(file, page);

					for(int i=0; i < num_of_cells; i++){
						
						long location = Page.getCellLocation(file, page, i);	
						String[] values = retrieve_data(file, location);
						int rowid=Integer.parseInt(values[0]);

						boolean check = expCheck(values, rowid, exp, column_name);
						
						if(check)
							buffer.add(rowid, values);
					}
				}
				else
					continue;
			}

			buffer.columnName = column_name;
			buffer.format = new int[column_name.length];

		}catch(Exception e){
			System.out.println("Error at filter");
			e.printStackTrace();
		}

	}

	public static void filter(RandomAccessFile file, String[] exp, String[] column_name, String[] type, Buffer buffer)
	{
		try{
			
			int num_of_pages = num_pages(file);
			
			for(int page = 1; page <= num_of_pages; page++){
				
				file.seek((page-1)*page_size);
				byte page_type = file.readByte();
				
					if(page_type == 0x0D){
						
					byte numOfCells = Page.get_cell_number(file, page);

					 for(int i=0; i < numOfCells; i++){
						long location = Page.getCellLocation(file, page, i);
						String[] values = retrieve_data(file, location);
						int rowid=Integer.parseInt(values[0]);
						
						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								values[j] = "'"+values[j]+"'";
						
						boolean check = expCheck(values, rowid , exp, column_name);

						
						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								values[j] = values[j].substring(1, values[j].length()-1);

						if(check)
							buffer.add(rowid, values);
					 }
				   }
				    else
						continue;
			}

			buffer.columnName = column_name;
			buffer.format = new int[column_name.length];

		}catch(Exception e){
			System.out.println("Error at filter");
			e.printStackTrace();
		}

	}


	
//createTable
	public static void createTable(String table, String[] columns)
	{
		try{	
			
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			file.setLength(page_size);
			file.seek(0);
			file.writeByte(0x0D);
			file.close();
				
			file = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
				
			int num_of_pages = num_pages(file);
			int page=1;
			for(int p = 1; p <= num_of_pages; p++){
				int right_most = Page.get_right_most(file, p);
				if(right_most == 0)
					page = p;
			}
				
			int[] keys = Page.get_key_array(file, page); //obtain all the keys in the table
			int l = keys[0];
			for(int i = 0; i < keys.length; i++) //find the max key
				if(keys[i]>l)
					l = keys[i];
			file.close();
				
			String[] values = {Integer.toString(l+1), table};
			insert_into("davisbase_tables", values);

			file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			
			num_of_pages = num_pages(file);
			page=1;
			for(int p = 1; p <= num_of_pages; p++){
				int right_most = Page.get_right_most(file, p);
				if(right_most == 0)
					page = p;
			}
				
			keys = Page.get_key_array(file, page);
			l = keys[0];
			for(int i = 0; i < keys.length; i++)
				if(keys[i]>l)
					l = keys[i];
			file.close();

			for(int i = 0; i < columns.length; i++){
				l = l + 1;
				String[] tokens = columns[i].split(" ");
				String column_name = tokens[0];
				String token_data = tokens[1].toUpperCase();
				String position = Integer.toString(i+1);
				String nullable;
				if(tokens.length > 2)
					nullable = "NO";
				else
					 nullable = "YES";
				String[] value = {Integer.toString(l), table, column_name, token_data, position, nullable};
				insert_into("davisbase_columns", value);
			}
		
		}catch(Exception e){
			System.out.println(e);
		}
	}


//insertInto
	public static void insert_into(String table, String[] values){
		try{
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			insert_into_file(file, table, values);
			file.close();

		}catch(Exception e){
			System.out.println(e);
		}
	}
//insertInto	
	public static void insert_into_file(RandomAccessFile file, String table, String[] values)
	{
		String[] data_type = get_data_type(table);
		String[] nullable = get_nullable(table);

		for(int i = 0; i < nullable.length; i++)
			if(values[i].equals("null") && nullable[i].equals("NO")){
				System.out.println("NULL-value constraint violation");
				System.out.println();
				return;
			}

		int row_id = new Integer(values[0]);
		int page = searchPageWithKey(file, row_id);
		if(page != 0)
			if(Page.hasRowId(file, page, row_id)){
				System.out.println("Uniqueness constraint violation");
				return;
			}
		if(page == 0)
			page = 1;


		byte[] type_of_data = new byte[data_type.length-1];
		short payload_size = (short) calculate_payload(table, values, type_of_data);
		int cell_size = payload_size + 6;
		int off = Page.checkSpaceInLeaf(file, page, cell_size);


		if(off != -1){
			Page.insertCellInLeaf(file, page, off, payload_size, row_id, type_of_data, values);
		}else{
			Page.splitLeaf(file, page);
			insert_into_file(file, table, values);
		}
	}

	public static void select(String table, String[] columns, String[] exp)
	{
		try{
			
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			String[] column_name = getColumnName(table);
			String[] type = get_data_type(table);
			
			Buffer buffer = new Buffer();
			
			filter(file, exp, column_name, type, buffer);
			buffer.display(columns);
			file.close();
		}catch(Exception e){
			System.out.println(e);
		}
	}


	
	public static void update(String table, String[] exp, String[] set)
	{
		try{
			
			int row_id = new Integer(exp[2]);
			
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			int num_of_pages = num_pages(file);
			int page = 0;
			for(int p = 1; p <= num_of_pages; p++)
				if(Page.hasRowId(file, p, row_id)&Page.get_type_of_page(file, p)==0x0D){
					page = p;
				}
			
			if(page==0)
			{
				System.out.println("The given key value does not exist");
				return;
			}
			
			int[] keys = Page.get_key_array(file, page);
			int k= 0;
			for(int i = 0; i < keys.length; i++)
				if(keys[i] == row_id)
					k = i;
			int off = Page.get_cell_offset(file, page, k);
			long location = Page.getCellLocation(file, page, k);
			
			String[] column_name = getColumnName(table);
			String[] values = retrieve_data(file, location);

			String[] type = get_data_type(table);
			for(int i=0; i < type.length; i++)
				if(type[i].equals("DATE") || type[i].equals("DATETIME"))
					values[i] = "'"+values[i]+"'";

			for(int i = 0; i < column_name.length; i++)
				if(column_name[i].equals(set[0]))
					k = i;
			values[k] = set[2];

			String[] nullable = get_nullable(table);
			for(int i = 0; i < nullable.length; i++){
				if(values[i].equals("null") && nullable[i].equals("NO")){
					System.out.println("NULL-value constraint violation");
					return;
				}
			}

			byte[] type_of_data = new byte[column_name.length-1];
			int payload_size = calculate_payload(table, values, type_of_data);
			Page.updateCellInLeaf(file, page, off, payload_size, row_id, type_of_data, values);

			file.close();

		}catch(Exception e){
			System.out.println(e);
		}
	}

	
//drop
	public static void drop(String table)
	{
		try{
			
			RandomAccessFile file = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			int num_of_pages = num_pages(file);
			for(int page = 1; page <= num_of_pages; page ++){
				file.seek((page-1)*page_size);
				byte file_type = file.readByte();
				if(file_type == 0x0D)
				{
					short[] cells_address = Page.get_cell_array(file, page);
					int j = 0;
					for(int i = 0; i < cells_address.length; i++)
					{
						long location = Page.getCellLocation(file, page, i);
						String[] values = retrieve_data(file, location);
						String table_name = values[1];
						if(!table_name.equals(table))
						{
							Page.set_cell_offset(file, page, j, cells_address[i]);
							j++;
						}
					}
					Page.set_cell_number(file, page, (byte)j);
				}
				else
					continue;
			}
			file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			num_of_pages = num_pages(file);
			for(int page = 1; page <= num_of_pages; page ++){
				file.seek((page-1)*page_size);
				byte file_type = file.readByte();
				if(file_type == 0x0D)
				{
					short[] cells_address = Page.get_cell_array(file, page);
					int k = 0;
					for(int i = 0; i < cells_address.length; i++)
					{
						long location = Page.getCellLocation(file, page, i);
						String[] values = retrieve_data(file, location);
						String table_name = values[1];
						if(!table_name.equals(table))
						{
							Page.set_cell_offset(file, page, k, cells_address[i]);
							k++;
						}
					}
					Page.set_cell_number(file, page, (byte)k);
				}
				else
					continue;
			}

			File oldFile = new File("data", table+".tbl"); 
			oldFile.delete();
		}catch(Exception e){
			System.out.println(e);
		}

	}
	    
	public static void delete(String table, String[] exp)
	{
		try{
		int key = new Integer(exp[2]);
		RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
		int num_of_pages = num_pages(file);
		int page = 0;
		for(int p = 1; p <= num_of_pages; p++)
			if(Page.hasRowId(file, p, key)&Page.get_type_of_page(file, p)==0x0D){
				page = p;
				break;
			}
		
		if(page==0)
		{
			System.out.println("The given key value does not exist");
			return;
		}
			
			short[] cells_address = Page.get_cell_array(file, page);
			int k = 0;
			for(int i = 0; i < cells_address.length; i++)
			{
				long location = Page.getCellLocation(file, page, i);
				String[] values = retrieve_data(file, location);
				int j = new Integer(values[0]);
				if(j!=key)
				{
					Page.set_cell_offset(file, page, k, cells_address[i]);
					k++;
				}
			}
			Page.set_cell_number(file, page, (byte)k);
			
		}catch(Exception e)
		{
			System.out.println(e);
		}
		
	}

	
}


	