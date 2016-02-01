/* *************************************************************** *
 * OzoneHadoop Project
 * Author : Luiz Angelo STEFFENEL <Luiz-Angelo.Steffenel@univ-reims.fr>
 * *************************************************************** */

package ozonecft.preproc;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * ParallelParser executable
 * This class is used to preprocess raw ozone data (in the format from ftp://toms.gsfc.nasa.gov/pub/omi/data)
 * and convert it to a more plain format that can be read by mapreduce
 * the generated format has the following form:
 * year month day latitude firstlongitude step measure measure measure ...
 * 
 * @author Luiz Angelo STEFFENEL <Luiz-Angelo.Steffenel@univ-reims.fr>
 */
public class ParallelParser {

    public static void main(String[] args) {

        File folder = new File(args[0]);
        File[] listOfFiles;

        int processors = Runtime.getRuntime().availableProcessors();
        
        ExecutorService executor = Executors.newFixedThreadPool(processors);

        
        if (folder.isDirectory()) {
            listOfFiles = folder.listFiles();
        } else {
            listOfFiles = new File[1];
            listOfFiles[0] = new File(args[0]);
        }

        for (int i = 0; i < listOfFiles.length; i++) {
           Runnable worker = new WorkerThread(listOfFiles[i]); 
           executor.execute(worker); 
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
            
        }
        System.out.println("Finished all threads");

    }

}
