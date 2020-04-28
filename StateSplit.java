package pastry_replica;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class StateSplit {


	static int numSpli;
	static int numReplic;
	
	
	private static  String filePath;
	private static int PART_SIZE1;
	
	public StateSplit(String filepath, int numParts) throws IOException {
        StateSplit.filePath = filepath;
        StateSplit.PART_SIZE1 = numParts;
 
	    }
	
	String FILE_NAME = filePath;
	//byte PART_SIZE = PART_SIZE1;
	
	public int stateSpliter() throws IOException  {
		File inputFile = new File(filePath);
		//System.out.println(filePath);
		FileInputStream inputStream;
		String newFileName;
		FileOutputStream filePart;
		long sourceSize = Files.size(Paths.get(filePath));
		long numSplits = sourceSize / (1024*PART_SIZE1);
		numSpli = (int) numSplits;
		int fileSize = (int) inputFile.length();
		int nChunks = 0, read = 0, readLength = 1024*PART_SIZE1;
		byte[] byteChunkPart;
		try {
			inputStream = new FileInputStream(inputFile);
			while (fileSize > 0) {
				if (fileSize <= 5) {
					readLength = fileSize;
				}
				if (fileSize <= readLength)
				{
					byteChunkPart = new byte[fileSize];
					read = inputStream.read(byteChunkPart, 0, fileSize);
				}else {
					byteChunkPart = new byte[readLength];
					read = inputStream.read(byteChunkPart, 0, readLength);
				}
				fileSize -= read;
				assert (read == byteChunkPart.length);
				nChunks++;
				newFileName = filePath + ".part_"
						+ Integer.toString(nChunks - 1) + "0";
				filePart = new FileOutputStream(new File(newFileName));
				System.out.println(newFileName);
				filePart.write(byteChunkPart);
				filePart.flush();
				filePart.close();
				byteChunkPart = null;
				filePart = null;
			}
			inputStream.close();
		} catch (IOException exception) {
			exception.printStackTrace();
		}
		return numSpli;
	}
	
	public void makeReplica( int num) throws IOException {
		int part_size = numSpli;
		int i,j =0;
		for (i = 0; i <= part_size; i++)
		{
			String sourceFile = filePath+".part_" + i + "0";
			int replica = num; // number of replica for each part
			

			for (j = 0; j < replica; j++) {
				String destFile = filePath+".part_" + i + (j+1); // name the replica as part0x
				InputStream is = null;
				OutputStream os = null;
			    try {
			        is = new FileInputStream(sourceFile);
			        os = new FileOutputStream(destFile);		    
			        byte[] buffer = new byte[1024];
			        int length;
			        while ((length = is.read(buffer)) > 0) {
			            os.write(buffer, 0, length);
			        }
			    } finally {
			        is.close();
			        os.close();
			    }
			}
		}
		

	}
}