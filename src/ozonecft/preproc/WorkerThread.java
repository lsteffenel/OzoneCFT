/* *************************************************************** *
 * OzoneHadoop Project
 * Author : Luiz Angelo STEFFENEL <Luiz-Angelo.Steffenel@univ-reims.fr>
 * *************************************************************** */
package ozonecft.preproc;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Luiz Angelo STEFFENEL <Luiz-Angelo.Steffenel@univ-reims.fr>
 */
public class WorkerThread implements Runnable {

    private File entryFile;

    public WorkerThread(File s) {
        this.entryFile = s;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + " Start. Command = " + entryFile);

        O3 o3 = parse(entryFile);

        String toto = new String();
        toto = toto.concat(Integer.toString(o3.getDate().get(Calendar.YEAR)) + " ");
        toto = toto.concat(((o3.getDate().get(Calendar.MONTH) + 1) < 10) ? "0" : "");
        toto = toto.concat(Integer.toString((o3.getDate().get(Calendar.MONTH) + 1)) + " ");
        toto = toto.concat(((o3.getDate().get(Calendar.DAY_OF_MONTH)) < 10) ? "0" : "");
        toto = toto.concat(Integer.toString(o3.getDate().get(Calendar.DAY_OF_MONTH)));

        String filename = toto.replace(" ", "");
        filename = filename.concat(".txt");
        FileHandler fh = new FileHandler(new File(filename));

        for (float lat = o3.getinitLat(); lat <= (-o3.getinitLat()); lat = (float) (lat + o3.getdLat())) {

            /* spaces */
                String line = new String(toto + " " + lat + " " + o3.getinitLon() + " " + o3.getdLon());
                for (float lon = o3.getinitLon(); lon <= (-o3.getinitLon()); lon = (float) (lon + o3.getdLon())) {
                    line = line.concat(" " + o3.getValue(lat, lon));
                }
                line = line.concat("\n");

            /* tabs */
//                String line = new String(toto + "\t" + lat + "\t" + o3.getinitLon() + "\t" + o3.getdLon());
//                for (float lon = o3.getinitLon(); lon <= (-o3.getinitLon()); lon = (float) (lon + o3.getdLon())) {
//                    line = line.concat("\t" + o3.getValue(lat, lon));
//                }
//                line = line.concat("\n");

            /* bag */
//            String line = new String(toto + "\t" + lat + "\t" + o3.getinitLon() + "\t" + o3.getdLon() + "\t{");
//            for (float lon = o3.getinitLon(); lon <= (-o3.getinitLon()); lon = (float) (lon + o3.getdLon())) {
//                line = line.concat("(" + o3.getValue(lat, lon) + ")");
//                if (lon<(-o3.getinitLon())) 
//                {
//                    line = line.concat(",");
//                }
//            }
//            line = line.concat("}\n");

            /* map */
//            String line = new String(toto + "\t" + lat + "\t" + o3.getinitLon() + "\t" + o3.getdLon()) + "\t[";
//            for (float lon = o3.getinitLon(); lon <= (-o3.getinitLon()); lon = (float) (lon + o3.getdLon())) {
//                line = line.concat(lon + "#" + o3.getValue(lat, lon));
//                if (lon<(-o3.getinitLon())) 
//                {
//                    line = line.concat(",");
//                }
//            }
//            line = line.concat("]\n");

            fh.writeLine(line);
//            System.out.println("");           
        }

        // fh.writeLine("test");
        fh.flushing();
        fh.close();

        System.out.println(Thread.currentThread().getName() + " End.");
    }

    O3 parse(File filename) {
        FileHandler fh = new FileHandler(filename);

        // header
        String line1 = fh.readLine();
        String line2 = fh.readLine();
        String line3 = fh.readLine();

        String[] tokens = line1.split("\\s+");
        // TODO : get date

        GregorianCalendar date = new GregorianCalendar();
        String formattedString = String.format("%s%s %s %s %s", tokens[4], tokens[3], tokens[5], tokens[13], tokens[14]);

        SimpleDateFormat sdf = new SimpleDateFormat("dd,MMM yyyy HH:mm a", Locale.ENGLISH);
        try {
            date.setTime(sdf.parse(formattedString));
        } catch (ParseException ex) {
            Logger.getLogger(WorkerThread.class.getName()).log(Level.SEVERE, null, ex);
        }

        //System.out.println(date.get(Calendar.DAY_OF_YEAR));
        tokens = line2.split("\\s+");

        Float initLon = Float.parseFloat(tokens[6]);
        Float dLon = Float.parseFloat(tokens[11].substring(1));

        tokens = line3.split("\\s+");

//        for (int i = 0; i < tokens.length;++i) {
//            System.out.println("["+i+"]"+tokens[i]+" ");
//        }
        Float initLat = Float.parseFloat(tokens[7]);
        Float dLat = Float.parseFloat(tokens[12].substring(1));

        //System.out.println(dLat+" "+dLon);
        O3 tmp = new O3(dLat, dLon);

        tmp.setDate(date);
        //tmp.setdLon(dLon);
        //tmp.setdLat(dLat);
        tmp.setLon(-initLon);
        tmp.setLat(-initLat);

        for (int latit = 0; latit < 180 / dLat; ++latit) {
            int longit = 0;
            while (longit < 360 / dLon) {
                //System.out.println(longit);
                String linedata = fh.readLine().substring(1);
                tokens = linedata.split("(?<=\\G.{3})"); //splits into chuncks of 3 char each
                for (int i = 0; i < 25; i++) {
                    if (longit < 360 / dLon) {
                        //System.out.print(tokens[i]+" ");

                        tmp.setOzone(latit, longit, Integer.parseInt(tokens[i].trim()));
                        longit++;
                    }
                }
                //System.out.println("");
            }

        }
        fh.close();
        return tmp;
    }

    @Override
    public String toString() {
        return this.entryFile.toString();
    }
}
