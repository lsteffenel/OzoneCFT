/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ozonecft;

import cloudfit.application.Distributed;
import cloudfit.storage.DHTStorageUnit;
import cloudfit.storage.FileContainer;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 * @author Luiz Angelo STEFFENEL <Luiz-Angelo.Steffenel@univ-reims.fr>
 */
public class OzoneFilter extends Distributed {

    private final boolean debug = false;

    private String[] sourcefiles = null;

    private coordinates[][] allcoords = null;

    /**
     * consumes a block of data, adding it to the result_accumulator. This
     * method can be compared to a <i>combiner</i> into Hadoop. It groups
     * results from a block, putting them into a shared variable, the
     * result_accumulator.
     *
     * @param result_accumulator
     * @todo Explain somewhere what is a segment and a block
     *
     * @param number block numeber in a segment (0 a <i>n</i> - 1).
     * @param value block content
     */
    @Override
    public void consumeBlock(Serializable result_accumulator, int number, Serializable value) {
        if (debug) {
            System.err.println("## consumeBlock " + number + ", " + value);
        }

        // ((MultiMap<K, V>) result_accumulator).putAll((MultiMap<K, V>) value);
    }

    /**
     * Finalizes the consumer, returing its accumulator.
     *
     * @return MultiMap<K, V> accumulator with calculated data
     */
    public Serializable finalizeApplication() {
        if (debug) {

            System.err.println("## finalizeConsumer ");

        }
        //return getAccumulator();
        return null;
    }

    @Override
    public Serializable initializeApplication() {
        //Serializable acc = new MultiMap<K, V>();

        String[] procargs = getArgs();
        System.err.println("args size = " + procargs.length);
        // left-upper latitude
        float lllat = Float.parseFloat(procargs[0]);
        // right-lower longitude
        float lllon = Float.parseFloat(procargs[1]);
        // right-lower latitude
        float rulat = Float.parseFloat(procargs[2]);
        // left-upper longitude
        float rulon = Float.parseFloat(procargs[3]);
        
        float step = Float.parseFloat(procargs[4]);
        //float step = 1;
        
        String dstart = procargs[5];
        String dend = procargs[6];
        String period = procargs[7];

        float latdist = rulat - lllat;
        float londist = rulon - lllon;

        int gridx = (int) Math.ceil(londist / step);
        int gridy = (int) Math.ceil(latdist / step);

        System.err.println("################" + gridx + " x " + gridy + "###########");
        System.out.println(".");

        allcoords = new coordinates[gridx][gridy];

        for (float x1 = lllon; x1 < rulon; x1=x1+step) {
            for (float y1 = lllat; y1 < rulat; y1=y1+step) {
                float x2 = x1 + step;
                float y2 = y1 + step;
                allcoords[Math.round(x1 - lllon)][Math.round(y1 - lllat)] = new coordinates(x1, x2, y1, y2);
                System.err.println(x1 + " <-> " + x2 + " / " + y1 + " <-> " + y2);
            }
        }

        int totalsize = procargs.length;
        int filesize = totalsize - 8;

        sourcefiles = new String[filesize];

        for (int i = 0; i < filesize; i++) {
            sourcefiles[i] = procargs[i + 8];
        }

        return gridx * gridy;

        //return acc;
    }

    /**
     * Evaluates the task <i>number</i> and returns <i>number</i> + 1.
     *
     * @param number task number (task id)
     * @param required data to be analysed
     * @return next task id
     */
    @Override
    public Serializable executeBlock(int number, Serializable[] required) {

        //setBlockParameters();
        long start, stop;

        start = System.currentTimeMillis();
        
        String result="";

        int myposy = (int) (number / allcoords.length);
        int myposx = number - (myposy * allcoords.length);

        ArrayList<mesures> al = new ArrayList<mesures>();

        coordinates mycoord = allcoords[myposx][myposy];

        //MultiMap<Float[],Integer[]> mmap = new MultiMap();
        for (int files = 0; files < sourcefiles.length; files++) {

            DHTStorageUnit titi = (DHTStorageUnit) read(sourcefiles[files]);

            if (titi == null) {
                System.out.println("Data not ready " + number);
                return null;
            }

            FileContainer fr = (FileContainer) titi.getContent();

            InputStream is = null;
            BufferedReader bfReader = null;
            try {
                is = new ByteArrayInputStream(fr.getContent());
                bfReader = new BufferedReader(new InputStreamReader(is));
                String line = null;
                while ((line = bfReader.readLine()) != null) {

                    String[] tokens = line.split("\\s+");

                    String linelat = tokens[3];
                    // if the measure cover the latitudes between rulat and lllat

                    if (Float.parseFloat(linelat) >= mycoord.y1 && Float.parseFloat(linelat) < mycoord.y2) {

//
                        float latitude = new Float(tokens[3]);
                        //FloatArrayWritable mykey = new FloatArrayWritable();
                        Float[] coords = new Float[2];
                        coords[0] = latitude;
                        Integer[] values = new Integer[4];

                        Integer year = new Integer(tokens[0]);
                        values[0] = year;
                        Integer month = new Integer(tokens[1]);
                        values[1] = month;
                        Integer day = new Integer(tokens[2]);
                        values[2] = day;

                        Integer mesure;
                        float lon;
                        // for each measure in the row, generates a <key,value> pair
                        for (int i = 6; i < tokens.length; ++i) {
                            lon = Float.parseFloat(tokens[4]);
                            lon = lon + ((i - 6) * (Float.parseFloat(tokens[5])));
                            // if the measure cover the longitudes between rolon and lllon
                            if (lon >= mycoord.x1 && lon < mycoord.x2) {
                                coords[1] = lon;

                                mesure = new Integer(tokens[i]);
                                values[3] = mesure;

                                //mmap.add(coords, values);
                                //System.out.println("(" + coords[0] + "," + coords[1] + ") - " + values[2] + " " + values[3]);

                                al.add(new mesures(coords[0], coords[1], values[0], values[1], values[2], values[3]));

                            }
                        }
                    }

                }

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (is != null) {
                        is.close();
                    }
                } catch (Exception ex) {

                }
            }
        }

        // Moyenne
        int sum = 0;
        int count = 0;
        if (!al.isEmpty()) {
            Iterator it = al.iterator();
            while (it.hasNext()) {
                mesures mes = (mesures) it.next();
                if (it.hasNext()) { // if we have n+1 days, we don't count the last day on the average
                    if (mes.value > 0) {
                        sum += mes.value;
                        count++;
                    }
                }
            }
        }
        float moyenne = (float) sum / count;
        //System.out.println("sum = " + sum + " ; count = " + count + " ; moyenne = " + moyenne);

        // stdev
        float diff = 0;
        if (!al.isEmpty()) {
            Iterator it = al.iterator();
            while (it.hasNext()) {
                mesures mes = (mesures) it.next();
                if (it.hasNext()) { // if we have n+1 days, we don't count the last day on the average
                    if (mes.value > 0) {
                        diff += (mes.value - moyenne) * (mes.value - moyenne);
                    }
                }
            }
        }

        double stdev = Math.sqrt(diff / count);
        //System.out.println("stedv = " + stdev + " ; moyenne = " + moyenne);

        
        // alert
        if (!al.isEmpty()) {
            
            mesures mes = (mesures)al.get(al.size()-1);
            
            //Iterator it = al.iterator();
            //while (it.hasNext()) {
            //    mesures mes = (mesures) it.next();
                if (mes.value > 0) {
                    if (mes.value < moyenne - (1.5 * stdev)) {
                        
                        //%08d%n
                        //System.out.format("%4d%02d%02d : (%.1f,%.1f) %d (moy=%f, stdev=%f) - EVENT%n",mes.yyyy,mes.mm,mes.dd,mes.lat,mes.lon,mes.value,moyenne,stdev);
                        System.out.format("%4d%02d%02d  %.1f %.1f %d %.3f %.3f EVENT%n",mes.yyyy,mes.mm,mes.dd,mes.lat,mes.lon,mes.value,moyenne,stdev);
                        result = String.format("%4d%02d%02d  %.1f %.1f %d %.3f %.3f EVENT%n",mes.yyyy,mes.mm,mes.dd,mes.lat,mes.lon,mes.value,moyenne,stdev);
                        //System.out.println(mes.yyyy +"" + mes.mm +""+ mes.dd + " : (" +mes.lat+","+mes.lon+") "+mes.value + "(moy=" + moyenne + ", stdev=" + stdev + ")");
                    }
                    else 
                    {
//                        System.out.format("%4d%02d%02d : (%.1f,%.1f) %d (moy=%f, stdev=%f)%n",mes.yyyy,mes.mm,mes.dd,mes.lat,mes.lon,mes.value,moyenne,stdev);
                        System.out.format("%4d%02d%02d  %.1f %.1f %d %.3f %.3f NORMAL%n",mes.yyyy,mes.mm,mes.dd,mes.lat,mes.lon,mes.value,moyenne,stdev);
                        result = String.format("%4d%02d%02d  %.1f %.1f %d %.3f %.3f NORMAL%n",mes.yyyy,mes.mm,mes.dd,mes.lat,mes.lon,mes.value,moyenne,stdev);
                     
                    }
                }
            //}
        }

        stop = System.currentTimeMillis();

        System.err.println("Task " + number + " done (" + (stop - start) + ")");

        return result;

    }

    /**
     * Evaluates the number of blocks in a resource segment. This number must be
     * always greater than 0.
     *
     */
    @Override
    public void numberOfBlocks() {

        //this.initializeApplication();
        int nb = (Integer) this.initializeApplication(); // Nombre de taches a renvoyer

        this.setNumberOfBlocks(nb);
    }

    private class coordinates implements Serializable {

        public float x1 = (float) -179.5;
        public float x2 = (float) 179.5;
        public float y1 = (float) -89.5;
        public float y2 = (float) 89.5;

        public coordinates(float x1, float x2, float y1, float y2) {
            this.x1 = x1;
            this.x2 = x2;
            this.y1 = y1;
            this.y2 = y2;
        }

    }

    private class mesures implements Serializable {

        public float lat = (float) -179.5;
        public float lon = (float) 179.5;
        public int value = 0;
        public int yyyy = 0;
        public int mm = 0;
        public int dd = 0;

        public mesures(float x1, float x2, int year, int month, int day, int val) {
            this.lat = x1;
            this.lon = x2;
            this.yyyy = year;
            this.mm = month;
            this.dd = day;
            this.value = val;
        }

    }
}
