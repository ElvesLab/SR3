package pastry_replica;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;

public class StateMerge  {
	
	
	static String filePath;
	public static int numSpl;
	public static String filePath1;
	
	public StateMerge(String filepath, int numSpl) throws IOException {
        StateMerge.filePath = filepath;
        StateMerge.numSpl = numSpl;
	    }
	
	
	@SuppressWarnings({ "null" })
	public void stateMerger() throws IOException {
		String rootTimeDir = StateMerge.filePath + File.separator + "merge_results";
		String FILE_NAME = StateMerge.filePath + File.separator + "merge_results"+ File.separator +  "original_data.txt";
  		File directory2 = new File(String.valueOf(rootTimeDir));
  		if(!directory2.exists()){
  			new File(rootTimeDir).mkdirs();
  		}
		File ofile = new File(FILE_NAME);
		FileOutputStream fos;
		FileInputStream fis;
		byte[] fileBytes;
		int bytesRead = 0;
		List<File> list = new ArrayList<File>();

		File directory = new File(StateMerge.filePath);
		File[] listOfFiles = directory.listFiles();
		List<String> realFiles = new ArrayList<String>();
		//List<File> realFiles = Arrays.asList(listOfFiles);
		//int[] indexPart = new int[20];
		//int numPart = 0;
		// Creating an empty TreeMap for parts 
        TreeMap<Integer, String> part_map 
            = new TreeMap<Integer, String>(); 
        
        for (File file : listOfFiles) {       	
            if (file.isFile()) {
                System.out.println(file.getName());
                //listOfSpeFiles.add(file);
                realFiles.add(file.getName());
            }else{ //if it is a directory
            	//realFiles.remove(file);
            }
        }
        System.out.println(realFiles.size());
		if ( realFiles.size() == (StateMerge.numSpl+1) ) {

		  for (String file : realFiles) { 
			  //String fileName = file.getName();
			  String[] parts = file.split("_");
			  
			  
			  if(parts.length > 1) { // means it has the "part" info, need to be merged
				  int firstPart = Integer.parseInt(parts[1])/10;// get the xx value of partxx
				  
				  if (part_map.get(firstPart) == null) {
					  part_map.put(firstPart, file);
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
		  
			for (Entry<Integer, String> m:part_map.entrySet()) {
				System.out.println("we have the part " + m.getValue());
				list.add(new File(StateMerge.filePath + File.separator + m.getValue()));
			} 
		}

			  
		try {
		    fos = new FileOutputStream(ofile,true);
		    for (File file : list) {
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
		        file.delete();
		    }
		    fos.close();
		    fos = null;
		    ofile.delete();//here for multiple recovery, so delete the merged one
		}catch (Exception exception){
			exception.printStackTrace();
		}
	}

}
