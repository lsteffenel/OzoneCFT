/* *************************************************************** *
 * OzoneHadoop Project
 * Author : Luiz Angelo STEFFENEL <Luiz-Angelo.Steffenel@univ-reims.fr>
 * *************************************************************** */
 
package ozonecft.preproc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * FileHandler allows easily reading or writing text files. Once the file is
 * open for reading (or writing), it can read (write) line by line, until it is
 * closed.
 *
 * @author kirsch
 */
public class FileHandler {
    
    public static final boolean READ = true;
    public static final boolean WRITE = false;
    
    /** file to be read / writen   */
    private File filename;
    
    /** input buffer (for reading) */
    private BufferedReader in;
    
    /** output buffer (for writing)  */
    private BufferedWriter /*PrintWriter*/ out;

    public FileHandler(File filename) {
        this.filename = filename;
    }

    public File getFilename() {
        return this.filename;
    }

    public void setFilename(File filename) {
        // if file is open, we close it
        if (this.isOpenForReading() || this.isOpenForWriting()) {
            this.close();
        } 
        
        this.filename = filename;
    }

    /**
     * reads a line on the file. If file is not open for reading, it opens a 
     * new stream for it.
     * 
     * <p><b>Attention: </b> if file is already open for writing, this method
     * will <i>not</i> close writing stream. It is up to developer to do so. </p>
     * 
     * @return String line read, or null if no more data available.
     */
    public String readLine() {
        String line = null;
        //should we close file for writing before open for reading or leave
        //this decision to the developper ?
        if (! this.isOpenForReading()) {
            this.open(READ);
        }
        
        try {
            line = this.in.readLine();
        } catch (IOException ioex) {
            line = null; //impossible to read a new line
        } catch (NullPointerException npe) {
            line = null; //open didn't work
        }
        
        return line;
    }

    /**
     * writes a line on the file. If file is not open for writing, it will do so. 
     * 
     * <p><b>Attention: </b> if file is already open for writing, this method
     * will <i>not</i> close writing stream. It is up to developer to do so.</p>
     * 
     * @param line String to be writen in the file
     * @return true if writing proceeds well, false if an exception raises. 
     */
    public boolean writeLine(String line) {
        boolean ok = false;
        
        if (! this.isOpenForWriting()) {
            this.open(WRITE);
        }
        
        try {
            this.out.append(line);
            ok = true;
        } catch (IOException ioex) {
            ok = false; //impossible to read a new line
        } catch (NullPointerException npe) {
            ok = false; //open didn't work
        }
        
        return ok;
    }

    /**
     * opens the file for reading or for writing, according parameter <i>read</i>
     * @param read true if file should be open for reading, false for writing
     * @return boolean true if file could be opened, false otherwise.
     */
    public boolean open(boolean read) {
        boolean ok = false;
        
        //first close the streams if opened
        if (this.isOpenForReading() || this.isOpenForWriting()) {
            this.close();
        }
        
        try {
          if(read == READ) { //open for reading
              this.in = new BufferedReader(new FileReader(this.filename));
          }  
          else {
              this.out = /*new PrintWriter (*/new BufferedWriter(new FileWriter(this.filename,false))/*)*/;
          }
          
          ok = true; //file is open
          
        } catch (IOException ioex) {
            //impossible to open the file
            ok = false;
            if (read) this.in = null;
            else this.out = null;
        }
        
        return ok;
    }

    /**
     * closes streams used to read / write the file
     * @return boolean true if it could close all streams, false otherwise
     */
    public boolean close() {
        boolean ok = true; //let's be optimistic
        
        try {
            if (this.in != null) {
                this.in.close();
                this.in = null;
            }
            if (this.out != null) {
                this.out.close();
                this.out = null;
            }
        } catch (IOException ex) {
            //impossible to close one of the streams
            ok = false;
            this.in = null;
            this.out = null;
        }

        return ok;
    }

    /**
     * checks if file is already open for reading.
     * @return true if an input stream is ready for use,
     *         false file should be open
     * @see #open
     */
    public boolean isOpenForReading() {
        return (this.in != null);
    }

    /**
     * checks if file is already open for writing.
     * @return true if an output stream is ready for use,
     *         false file should be open
     * @see #open
     */
    public boolean isOpenForWriting() {
        return (this.out != null);
    }

    /**
     * flushes output buffer.
     * @return true if flush could be done (if it doesn't rise any exception), 
     * false otherwise. 
     */
    public boolean flushing() {
        boolean ok = false;
        if (this.isOpenForWriting()) {
            try {
                this.out.flush();
                ok = true;
            } catch (IOException ex) {
                //couldn't flush
                ok = false;
            }
        }
        return ok;
    }
    
    /**
     * tests if input buffer is ready for reading.
     * @return true if buffer is ready, false otherwise (file not opened or 
     * no data available).
     */
    public boolean ready() {
        boolean ok = false;
        
        if (this.isOpenForReading()) {
            try {
                ok = this.in.ready();
                ok = true;
            } catch (IOException ex) {
                ok = false;
            }
        }
        
        return ok;
    }
    
    
}