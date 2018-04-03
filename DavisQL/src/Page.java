import java.io.RandomAccessFile;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Page{
	public static int page_size = 512;
	public static final String date_pattern = "yyyy-MM-dd_HH:mm:ss";

	public static short calculate_payload(String[] values, String[] type_of_data)
	{
		int type = type_of_data.length; 
		for(int i = 1; i < type_of_data.length; i++){
			String dt = type_of_data[i];
			switch(dt){
				case "TINYINT":
					type = type + 1;
					break;
				case "SMALLINT":
					type = type + 2;
					break;
				case "INT":
					type = type + 4;
					break;
				case "BIGINT":
					type = type + 8;
					break;
				case "REAL":
					type = type + 4;
					break;		
				case "DOUBLE":
					type = type + 8;
					break;
				case "DATETIME":
					type = type + 8;
					break;
				case "DATE":
					type = type + 8;
					break;
				case "TEXT":
					String text = values[i];
					int length = text.length();
					type = type + length;
					break;
				default:
					break;
			}
		}
		return (short)type;
	}

//findMidKey
	public static int findMiddleKey(RandomAccessFile file, int page){
		int value = 0;
		try{
			file.seek((page-1)*page_size);
			byte page_type = file.readByte();
			int num_of_cells = get_cell_number(file, page);
			int middle = (int) Math.ceil((double) num_of_cells / 2);
			long location = getCellLocation(file, page, middle-1);
			file.seek(location);

			switch(page_type){
				case 0x05:
					file.readInt(); 
					value = file.readInt();
					break;
				case 0x0D:
					file.readShort();
					value = file.readInt();
					break;
				}
			}catch(Exception e){
			System.out.println(e);
		}

		return value;
	}

	public static int makeInteriorPage(RandomAccessFile file)
	{
		int num_of_pages = 0;
		try{
			num_of_pages = (int)(file.length()/(new Long(page_size)));
			num_of_pages = num_of_pages + 1;
			file.setLength(page_size * num_of_pages);
			file.seek((num_of_pages-1)*page_size);
			file.writeByte(0x05); 
		}catch(Exception e){
			System.out.println(e);
		}

		return num_of_pages;
	}

	public static int makeLeafPage(RandomAccessFile file)
	{
		int num_of_pages = 0;
		try{
			num_of_pages = (int)(file.length()/(new Long(page_size)));
			num_of_pages = num_of_pages + 1;
			file.setLength(page_size * num_of_pages);
			file.seek((num_of_pages-1)*page_size);
			file.writeByte(0x0D); 
		}catch(Exception e){
			System.out.println(e);
		}

		return num_of_pages;

	}



	public static void splitInteriorPage(RandomAccessFile file, int current_page, int new_page)
	{
		try{
			
			int num_of_cells = get_cell_number(file, current_page);
			
			int middle = (int) Math.ceil((double) num_of_cells / 2);

			int cell_num_A = middle - 1;
			int cell_num_B = num_of_cells - cell_num_A - 1;
			short content = 512;

			for(int i = cell_num_A+1; i < num_of_cells; i++){
				long location = getCellLocation(file, current_page, i);
				short cell_size = 8;
				content = (short)(content - cell_size);
				file.seek(location);
				byte[] new_cell = new byte[cell_size];
				file.read(new_cell);
				file.seek((new_page-1)*page_size+content);
				file.write(new_cell);
				file.seek(location);
				int page = file.readInt();
				set_parent(file, page, new_page);
				set_cell_offset(file, new_page, i - (cell_num_A + 1), content);
			}
			
			int temp = get_right_most(file, current_page);
			set_right_most(file, new_page, temp);
			
			long middle_location = getCellLocation(file, current_page, middle - 1);
			file.seek(middle_location);
			temp = file.readInt();
			set_right_most(file, current_page, temp);
			
			file.seek((new_page-1)*page_size+2);
			file.writeShort(content);
			
			short off = get_cell_offset(file, current_page, cell_num_A-1);
			file.seek((current_page-1)*page_size+2);
			file.writeShort(off);

			
			int parent = get_parent(file, current_page);
			set_parent(file, new_page, parent);
			
			byte number = (byte) cell_num_A;
			set_cell_number(file, current_page, number);
			number = (byte) cell_num_B;
			set_cell_number(file, new_page, number);
			
		}catch(Exception e){
			System.out.println(e);
		}
	}

	public static void splitLeafPage(RandomAccessFile file, int current_page, int new_page)
	{
		try{
			
			int num_of_cells = get_cell_number(file, current_page);
			
			int middle = (int) Math.ceil((double) num_of_cells / 2);

			int cell_num_A = middle - 1;
			int cell_num_B = num_of_cells - cell_num_A;
			int content = 512;

			for(int i = cell_num_A; i < num_of_cells; i++){
				long location = getCellLocation(file, current_page, i);
				file.seek(location);
				int cell_size = file.readShort()+6;
				content = content - cell_size;
				file.seek(location);
				byte[] new_cell = new byte[cell_size];
				file.read(new_cell);
				file.seek((new_page-1)*page_size+content);
				file.write(new_cell);
				set_cell_offset(file, new_page, i - cell_num_A, content);
			}

			
			file.seek((new_page-1)*page_size+2);
			file.writeShort(content);

			
			short off = get_cell_offset(file, current_page, cell_num_A-1);
			file.seek((current_page-1)*page_size+2);
			file.writeShort(off);

			
			int right_most = get_right_most(file, current_page);
			set_right_most(file, new_page, right_most);
			set_right_most(file, current_page, new_page);

			
			int parent = get_parent(file, current_page);
			set_parent(file, new_page, parent);

			
			byte number = (byte) cell_num_A;
			set_cell_number(file, current_page, number);
			number = (byte) cell_num_B;
			set_cell_number(file, new_page, number);
			
		}catch(Exception e){
			System.out.println(e);
			
		}
	}

	
	public static int splitInteriorNode(RandomAccessFile file, int page)
	{
		int new_page = makeInteriorPage(file);
		int middle_key = findMiddleKey(file, page);
		splitInteriorPage(file, page, new_page);
		int parent = get_parent(file, page);
		if(parent == 0){
			int root_page = makeInteriorPage(file);
			set_parent(file, page, root_page);
			set_parent(file, new_page, root_page);
			set_right_most(file, root_page, new_page);
			insertCellInInteriorNode(file, root_page, page, middle_key);
			return root_page;
		}else{
			long pointer_location = getPointerLocation(file, page, parent);
			setPointerLocation(file, pointer_location, parent, new_page);
			insertCellInInteriorNode(file, parent, page, middle_key);
			sort_cell_array(file, parent);
			return parent;
		}
	}

	public static void splitLeaf(RandomAccessFile file, int page)
	{
		int new_page = makeLeafPage(file);
		int middle_key = findMiddleKey(file, page);
		splitLeafPage(file, page, new_page);
		int parent = get_parent(file, page);
		if(parent == 0){
			int root_page = makeInteriorPage(file);
			set_parent(file, page, root_page);
			set_parent(file, new_page, root_page);
			set_right_most(file, root_page, new_page);
			insertCellInInteriorNode(file, root_page, page, middle_key);
		}else{
			long pointer_location = getPointerLocation(file, page, parent);
			setPointerLocation(file, pointer_location, parent, new_page);
			insertCellInInteriorNode(file, parent, page, middle_key);
			sort_cell_array(file, parent);
			while(checkSpaceInInteriorNode(file, parent)){
				parent = splitInteriorNode(file, parent);
			}
		}
	}


//sortCellArray
	public static void sort_cell_array(RandomAccessFile file, int page){
		 byte number = get_cell_number(file, page);
		 int[] key_array = get_key_array(file, page);
		 short[] cell_array = get_cell_array(file, page);
		 int l_temp;
		 short r_temp;

		 for (int i = 1; i < number; i++) {
            for(int j = i ; j > 0 ; j--){
                if(key_array[j] < key_array[j-1]){

                	r_temp = (short) key_array[j];
                    key_array[j] = key_array[j-1];
                    key_array[j-1] = r_temp;
                	
                    l_temp = key_array[j];
                    key_array[j] = key_array[j-1];
                    key_array[j-1] = l_temp;

                }
            }
         }

         try{
         	file.seek((page-1)*page_size+12);
         	for(int i = 0; i < number; i++){
				file.writeShort(key_array[i]);
			}
         }catch(Exception e){
         	System.out.println("Error at sort_cell_array");
         }
	}
//getKeyArray
	public static int[] get_key_array(RandomAccessFile file, int page)
	{
		int number = new Integer(get_cell_number(file, page));
		int[] arr = new int[number];

		try{
			file.seek((page-1)*page_size);
			byte page_type = file.readByte();
			byte offset = 0;
			switch(page_type){
			    case 0x0d:
				    offset = 2;
				    break;
				case 0x05:
					offset = 4;
					break;
				default:
					offset = 2;
					break;
			}

			for(int i = 0; i < number; i++){
				long location = getCellLocation(file, page, i);
				file.seek(location+offset);
				arr[i] = file.readInt();
			}

		}catch(Exception e){
			System.out.println(e);
		}

		return arr;
	}
//getCellArray
	public static short[] get_cell_array(RandomAccessFile file, int page)
	{
		int number = new Integer(get_cell_number(file, page));
		short[] arr = new short[number];

		try{
			file.seek((page-1)*page_size+12);
			for(int i = 0; i < number; i++){
				arr[i] = file.readShort();
			}
		}catch(Exception e){
			System.out.println(e);
		}

		return arr;
	}

//getPointerLoc	
	public static long getPointerLocation(RandomAccessFile file, int page, int parent)
	{
		long value = 0;
		try{
			int num_of_cells = new Integer(get_cell_number(file, parent));
			for(int i=0; i < num_of_cells; i++){
				long location = getCellLocation(file, parent, i);
				file.seek(location);
				int childPage = file.readInt();
				if(childPage == page){
					value = location;
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}

		return value;
	}
//setPointerLoc
	public static void setPointerLocation(RandomAccessFile file, long location, int parent, int page)
	{
		try{
			if(location == 0){
				file.seek((parent-1)*page_size+4);
			}else{
				file.seek(location);
			}
			file.writeInt(page);
		}catch(Exception e){
			System.out.println(e);
		}
	} 

//insertInteriorCell	
	public static void insertCellInInteriorNode(RandomAccessFile file, int page, int child, int rowid)
	{
		try{
			
			file.seek((page-1)*page_size+2);
			short content = file.readShort();
			
			if(content == 0)
				content = 512;
			
			content = (short)(content - 8);
			
			file.seek((page-1)*page_size+content);
			file.writeInt(child);
			file.writeInt(rowid);
			
			file.seek((page-1)*page_size+2);
			file.writeShort(content);
			
			byte number = get_cell_number(file, page);
			set_cell_offset(file, page ,number, content);
			
			number = (byte) (number + 1);
			set_cell_number(file, page, number);

		}catch(Exception e){
			System.out.println(e);
		}
	}
//insertLeafCell
	public static void insertCellInLeaf(RandomAccessFile file, int page, int offset, short payload_size, int rowid, byte[] type_of_data, String[] values)
	{
		try{
			String str;
			file.seek((page-1)*page_size+offset);
			file.writeShort(payload_size);
			file.writeInt(rowid);
			int column = values.length - 1;
			file.writeByte(column);
			file.write(type_of_data);
			for(int i = 1; i < values.length; i++){
				switch(type_of_data[i-1]){
					case 0x00:
						file.writeByte(0);
						break;
					case 0x01:
						file.writeShort(0);
						break;
					case 0x02:
						file.writeInt(0);
						break;
					case 0x03:
						file.writeLong(0);
						break;
					case 0x04:
						file.writeByte(new Byte(values[i]));
						break;
					case 0x05:
						file.writeShort(new Short(values[i]));
						break;
					case 0x06:
						file.writeInt(new Integer(values[i]));
						break;
					case 0x07:
						file.writeLong(new Long(values[i]));
						break;
					case 0x08:
						file.writeFloat(new Float(values[i]));
						break;
					case 0x09:
						file.writeDouble(new Double(values[i]));
						break;
					case 0x0A:
						str = values[i];
						Date temp_date = new SimpleDateFormat(date_pattern).parse(str.substring(1, str.length()-1));
						long time = temp_date.getTime();
						file.writeLong(time);
						break;
					case 0x0B:
						str = values[i];
						str = str.substring(1, str.length()-1);
						str = str+"_00:00:00";
						Date temp_date2 = new SimpleDateFormat(date_pattern).parse(str);
						long time2 = temp_date2.getTime();
						file.writeLong(time2);
						break;
					default:
						file.writeBytes(values[i]);
						break;
				}
			}
			int cell_num = get_cell_number(file, page);
			byte temp = (byte) (cell_num+1);
			set_cell_number(file, page, temp);
			file.seek((page-1)*page_size+12+cell_num*2);
			file.writeShort(offset);
			file.seek((page-1)*page_size+2);
			int content = file.readShort();
			if(content >= offset || content == 0){
				file.seek((page-1)*page_size+2);
				file.writeShort(offset);
			}
		}catch(Exception e){
			System.out.println(e);
		}
	}
//updateLeafCell
	public static void updateCellInLeaf(RandomAccessFile file, int page, int offset, int payload_size, int rowid, byte[] type_of_data, String[] values)
	{
		try{
			String str;
			file.seek((page-1)*page_size+offset);
			file.writeShort(payload_size);
			file.writeInt(rowid);
			int column = values.length - 1;
			file.writeByte(column);
			file.write(type_of_data);
			for(int i = 1; i < values.length; i++){
				switch(type_of_data[i-1]){
					case 0x00:
						file.writeByte(0);
						break;
					case 0x01:
						file.writeShort(0);
						break;
					case 0x02:
						file.writeInt(0);
						break;
					case 0x03:
						file.writeLong(0);
						break;
					case 0x04:
						file.writeByte(new Byte(values[i]));
						break;
					case 0x05:
						file.writeShort(new Short(values[i]));
						break;
					case 0x06:
						file.writeInt(new Integer(values[i]));
						break;
					case 0x07:
						file.writeLong(new Long(values[i]));
						break;
					case 0x08:
						file.writeFloat(new Float(values[i]));
						break;
					case 0x09:
						file.writeDouble(new Double(values[i]));
						break;
					case 0x0A:
						str = values[i];
						Date temp_date = new SimpleDateFormat(date_pattern).parse(str.substring(1, str.length()-1));
						long time = temp_date.getTime();
						file.writeLong(time);
						break;
					case 0x0B:
						str = values[i];
						str = str.substring(1, str.length()-1);
						str = str+"_00:00:00";
						Date temp_date2 = new SimpleDateFormat(date_pattern).parse(str);
						long time2 = temp_date2.getTime();
						file.writeLong(time2);
						break;
					default:
						file.writeBytes(values[i]);
						break;
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}
	}

//checkLeafSpace
	public static int checkSpaceInLeaf(RandomAccessFile file, int page, int size)
	{
		int value = -1;

		try{
			file.seek((page-1)*page_size+2);
			int content = file.readShort();
			if(content == 0)
				return page_size - size;
			int num_of_cells = get_cell_number(file, page);
			int space = content - 20 - 2*num_of_cells;
			if(size < space)
				return content - size;
			
		}catch(Exception e){
			System.out.println(e);
		}

		return value;
	}
//checkInteriorSpace	
	public static boolean checkSpaceInInteriorNode(RandomAccessFile file, int page)
	{
		byte num_of_cells = get_cell_number(file, page);
		if(num_of_cells > 30)
			return true;
		else
			return false;
	}

	
//getRightMost	
	public static int get_right_most(RandomAccessFile file, int page)
	{
		int right_most = 0;

		try{
			file.seek((page-1)*page_size+4);
			right_most = file.readInt();
		}catch(Exception e){
			System.out.println("Error at get_right_most");
		}

		return right_most;
	}
//setRightMost
	public static void set_right_most(RandomAccessFile file, int page, int rightLeaf)
	{

		try{
			file.seek((page-1)*page_size+4);
			file.writeInt(rightLeaf);
		}catch(Exception e){
			System.out.println("Error at set_right_most");
		}

	}
//getParent
	public static int get_parent(RandomAccessFile file, int page)
	{
		int value = 0;

		try{
			file.seek((page-1)*page_size+8);
			value = file.readInt();
		}catch(Exception e){
			System.out.println(e);
		}
		return value;
	}
//setParent
	public static void set_parent(RandomAccessFile file, int page, int parent)
	{
		try{
			file.seek((page-1)*page_size+8);
			file.writeInt(parent);
		}catch(Exception e){
			System.out.println(e);
		}
	}

//getCellLoc	
	public static long getCellLocation(RandomAccessFile file, int page, int id)
	{
		long location = 0;
		try{
			file.seek((page-1)*page_size+12+id*2);
			short off = file.readShort();
			long origin = (page-1)*page_size;
			location = origin + off;
		}catch(Exception e){
			System.out.println(e);
		}
		return location;
	}
//hasKey
	public static boolean hasRowId(RandomAccessFile file, int page, int rowid)
	{
		int[] keys = get_key_array(file, page);
		for(int i : keys)
			if(rowid == i)
				return true;
		return false;
	}

//getPageType    
	public static byte get_type_of_page(RandomAccessFile file, int page)
	{
		byte type=0x05;
		try {
			file.seek((page-1)*page_size);
			type = file.readByte();
		} catch (Exception e) {
			System.out.println(e);
		}
		return type;
	}
	
//setCellNumber
	public static void set_cell_number(RandomAccessFile file, int page, byte number)
	{
		try{
			file.seek((page-1)*page_size+1);
			file.writeByte(number);
		}catch(Exception e){
			System.out.println(e);
		}
	}
//getCellNumber
	public static byte get_cell_number(RandomAccessFile file, int page)
	{
		byte value = 0;
		try{
			file.seek((page-1)*page_size+1);
			value = file.readByte();
		}catch(Exception e){
			System.out.println(e);
		}
		return value;
	}

//getCellOffset	
	public static short get_cell_offset(RandomAccessFile file, int page, int id)
	{
		short off = 0;
		try{
			file.seek((page-1)*page_size+12+id*2);
			off = file.readShort();
		}catch(Exception e){
			System.out.println(e);
		}
		return off;
	}

//setCellOffset
	public static void set_cell_offset(RandomAccessFile file, int page, int id, int off)
	{
		try{
			file.seek((page-1)*page_size+12+id*2);
			file.writeShort(off);
		}catch(Exception e){
			System.out.println(e);
		}
	}
	
	public static void main(String[] args){}
}
