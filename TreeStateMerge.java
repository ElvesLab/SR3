package pastry_replica;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;


public class TreeStateMerge  {
	
	
	static String filePath1;
	//public static int numSpl;
	
	public TreeStateMerge(String filepath) throws IOException {
        StateMerge.filePath1 = filepath;
        //FileMerge.numSpl = numSpl;
	    }
	
	
	
	public void stateMerger() throws IOException {
		String FILE_NAME = StateMerge.filePath1 + File.separator + "original_data.txt";
		//File ofile = new File(FILE_NAME);
		FileOutputStream fos;
		FileInputStream fis;
		byte[] fileBytes;
		int bytesRead = 0;
		List<File> list = new ArrayList<File>();

		File directory = new File(StateMerge.filePath1);
		File[] listOfFiles = directory.listFiles();
		//int[] indexPart = new int[20];
		//int numPart = 0;
		// Creating an empty TreeMap for parts 
		NavigableMap<Integer, String> part_map 
            = new TreeMap<Integer, String>(); 
		TreeMap<Integer, String> merge_map = new TreeMap<Integer, String>();
		
        for (File file : listOfFiles) {
            if (file.isFile()) {
                System.out.println(file.getName());
            }
        }
        int merge_begin = 0;
        int merge_end = 0;
        for (File file : listOfFiles) { 
			  String fileName = file.getName();
			  String[] parts = fileName.split("_");
			  
			  //here first to check if some parts had been merged or not
			  
			  if (parts.length == 5) {//it does have merged parts from its children (merged_begin_2_to_4)
				  merge_begin = Integer.parseInt(parts[2]); //the begin part that has been merged (2)
				  merge_end = Integer.parseInt(parts[4]); // the end part that has been merged (4)
				  //for (int i = merge_begin; i<= merge_end ; i++) {
				  merge_map.put(merge_begin, fileName); // this part has been merged, do not need again
				  merge_map.put(merge_end, fileName); // this part has been merged, do not need again
				  part_map.put(merge_begin, fileName);
				  part_map.put(merge_end, fileName);
				  //}				  
			  }
        }
		  for (File file : listOfFiles) { 
			  String fileName = file.getName();
			  String[] parts = fileName.split("_");
			  
			  //here first to check if some parts had been merged or not
			  
			  if (parts.length == 5) {//it does have merged parts from its children (merged_begin_2_to_4)
				  merge_begin = Integer.parseInt(parts[2]); //the begin part that has been merged (2)
				  merge_end = Integer.parseInt(parts[4]); // the end part that has been merged (4)
				  //for (int i = merge_begin; i<= merge_end ; i++) {
				  merge_map.put(merge_begin, fileName); // this part has been merged, do not need again
				  merge_map.put(merge_end, fileName); // this part has been merged, do not need again
				  part_map.put(merge_begin, fileName);
				  part_map.put(merge_end, fileName);
				  //}				  
			  }
			  			  
			  if(parts.length == 2) { // means it has the "part" info, need to be merged
				  int firstPart = Integer.parseInt(parts[1])/10;// get the xx value of partxx
				  
				  if (part_map.get(firstPart) == null ) {
					  part_map.put(firstPart, fileName);
					  System.out.println("I find the " + firstPart + " part!");
				  }
				  //if (indexPart[numPart] != firstPart || firstPart == 0 ) {// note that this part has not been merged
					  //if (firstPart != 0 )
						  //System.out.println("Oops, it doesnot start with first part, it starts with " + firstPart + " part!");
					  //indexPart[numPart] = firstPart;
					  //System.out.println("I am now merging the " + firstPart + " part!");
					  //list.add(new File(filePath+"\\replica_part_"+parts[2]));
				  //}
			  }
		  }
		  
			//after sorting all replicas, now save the info that can be merged
		  	Entry<Integer, String> firstPart = part_map.firstEntry();
		  	//int k = firstPart.getKey();
			for (Entry<Integer, String> m:part_map.entrySet()) {
				if (part_map.higherEntry(m.getKey()) != null) {
					System.out.println("we have the part: " + m.getKey() +" and next part: " + part_map.higherEntry(m.getKey()).getKey() + " size of part_map: " + part_map.size());
					int j = part_map.higherEntry(m.getKey()).getKey() - 1 ;
					if (part_map.size() > 1 && m.getKey() == j  ) { // here to check if this two parts are consecutive
						//list.add(new File(filePath + "\\" + m.getValue()));
						//Map.Entry<Integer, String> next = part_map.higherEntry(m.getKey());
						//list.add(new File(filePath + "\\" + next.getValue()));
						merge_map.put(m.getKey(), m.getValue());
					}else if ( merge_map.size()>=2 && merge_map.size() != part_map.size() ) {// here merge the consecutive parts in the node
						Entry<Integer, String> begin = merge_map.firstEntry();
					  	int mergeBegin = begin.getKey();	
						int mergeEnd = 0;
						for (Entry<Integer, String> n:merge_map.entrySet()) {						
							list.add(new File(StateMerge.filePath1 + File.separator + n.getValue()));														
							mergeEnd = n.getKey();
						}
						// Just in case, if it records twice for merged_from_xx_to_xx (from the merge_map)
						List<File> dedupedList = list.stream().distinct().collect(Collectors.toList()); // remove the duplicate part names in the list
						list.clear();
						merge_map.clear();
						
						String File_Merged = StateMerge.filePath1 + File.separator + "merged_begin_"+ mergeBegin +"_to_"+ mergeEnd;
						File ofile = new File(File_Merged);	  
					try {
					    fos = new FileOutputStream(ofile,true);
					    									    
					    for (File file : dedupedList) {
					        fis = new FileInputStream(file);
					        fileBytes = new byte[(int) file.length()];
					        bytesRead = fis.read(fileBytes, 0,(int)  file.length());
					        assert(bytesRead == fileBytes.length);
					        assert(bytesRead == (int) file.length());
					        fos.write(fileBytes);
					        fos.flush();
					        fileBytes = null;
					        fis.close();
					        fis = null;
					        //here delete the original merged parts
					        System.out.println( file + " has been merged, done");
					        //file.delete();
					    }
					    fos.close();
					    fos = null;
					    dedupedList.clear(); // here clear the info in the merged list
					}catch (Exception exception){
						exception.printStackTrace();
						}
					}
					
				}else if ( part_map.lowerEntry(m.getKey()) != null && m.getKey() == part_map.lowerEntry(m.getKey()).getKey() + 1) {
					// here has no higher entry, its the last one, add into the merge_map
					merge_map.put(m.getKey(), m.getValue());
				} 
			}
			if (merge_map.size() == part_map.size()) {// here for node has no merged part, only has original parts that equals to original file
				Entry<Integer, String> begin = merge_map.firstEntry();
			  	int mergeBegin = begin.getKey();					
				int mergeEnd = 0;
				for (Entry<Integer, String> n:merge_map.entrySet()) {
					list.add(new File(StateMerge.filePath1 + File.separator + n.getValue()));
					mergeEnd = n.getKey();
				}
				// Just in case, if it records twice for merged_from_xx_to_xx (from the merge_map)
				List<File> dedupedList = list.stream().distinct().collect(Collectors.toList()); // remove the duplicate part names in the list
				System.out.println("Now the list for the merged parts are: " + dedupedList);
				list.clear();
				merge_map.clear();
				
				String File_Merged = StateMerge.filePath1 + File.separator + "merged_begin_"+ mergeBegin +"_to_"+ mergeEnd;
				File ofile = new File(File_Merged);	  
			try {
				
			    fos = new FileOutputStream(ofile,true);
			    for (File file : dedupedList) {
			        fis = new FileInputStream(file);
			        fileBytes = new byte[(int) file.length()];
			        bytesRead = fis.read(fileBytes, 0,(int)  file.length());
			        assert(bytesRead == fileBytes.length);
			        assert(bytesRead == (int) file.length());
			        fos.write(fileBytes);
			        fos.flush();
			        fileBytes = null;
			        fis.close();
			        fis = null;
			        System.out.println( file + " has been merged, so we delete it");
			        file.delete();
			    }
			    fos.close();
			    fos = null;
			    dedupedList.clear(); // here clear the info in the merged list
			}catch (Exception exception){
				exception.printStackTrace();
				}
			}			
	}
}
