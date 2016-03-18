package ozonecft;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import cloudfit.core.CoreORB;
import cloudfit.core.CoreQueue;
import cloudfit.core.TheBigFactory;
import cloudfit.network.NetworkAdapterInterface;
import cloudfit.network.TomP2PAdapter;
import cloudfit.service.Community;
import cloudfit.storage.DHTStorageUnit;
import cloudfit.storage.FileContainer;
import cloudfit.storage.StorageAdapterInterface;
import cloudfit.util.Number160;
import java.io.File;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 *
 * @author Luiz Angelo STEFFENEL <Luiz-Angelo.Steffenel@univ-reims.fr>
 */
public class Submitter {

    private static List<String> filenames = new java.util.concurrent.CopyOnWriteArrayList<String>();
    private static Community community;
    private static CoreORB TDTR;

    public static void main(String[] args) {
        long start;
        long end;

        Options options = new Options();
        Option help = new Option("help", "print this message");
        Option src = OptionBuilder.withArgName("src")
                .hasArg()
                .withDescription("source directory for data")
                .create("src");
        //sourceDir.setRequired(true);
        Option jar = OptionBuilder.withArgName("jar")
                .hasArg()
                .withDescription("submits a jar to execute")
                .create("jar");
        Option runclass = OptionBuilder.withArgName("runclass")
                .hasArg()
                .withDescription("class to execute (from the jar)")
                .create("runclass");

        Option dates = OptionBuilder.withArgName("dates")
                .hasArg()
                .withDescription("dates to consider (from x to x)")
                .create("dates");

        Option coords = OptionBuilder.withArgName("coords")
                .hasArg()
                .withDescription("limit coords - lllat lllon rulat rolon")
                .create("coords");

        Option period = OptionBuilder.withArgName("period")
                .hasArg()
                .withDescription("interval to analyse (15, 30 days)")
                .create("period");

        Option step = OptionBuilder.withArgName("step")
                .hasArg()
                .withDescription("the grid step (1 degree, 0.25 degrees)")
                .create("step");
        
        Option node = OptionBuilder.withArgName("node")
                .hasArg()
                .withDescription("Optional address to join the P2P network")
                .create("node");
        Option port = OptionBuilder.withArgName("port")
                .hasArg()
                .withDescription("Optional port to join the P2P network")
                .create("port");

        options.addOption(help);
        options.addOption(src);
        options.addOption(jar);
        options.addOption(runclass);
        options.addOption(coords);
        options.addOption(period);
        options.addOption(step);
        options.addOption(dates);
        options.addOption(node);
        options.addOption(port);

        // create the parser
        CommandLineParser parser = new PosixParser();
        CommandLine line = null;
        try {
            // parse the command line arguments
            line = parser.parse(options, args);
        } catch (ParseException exp) {
            // oops, something went wrong
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            usage(options);
            return;
        }

        InetSocketAddress peer = null; // the defaut value = discovery

        if (line.hasOption("node")) {
            if (line.hasOption("port")) {
                peer = new InetSocketAddress(line.getOptionValue("node"), Integer.parseInt(line.getOptionValue("port")));
            } else {
                peer = new InetSocketAddress(line.getOptionValue("node"), 7777);
            }

        } else {
            peer = new InetSocketAddress(InetAddress.getLoopbackAddress(), 7777);
        }

        initNetwork(peer);

        start = System.currentTimeMillis();

        if (!(line.hasOption("src") && line.hasOption("jar") && line.hasOption("dates"))) {
            System.out.println("minimum args are src, jar and dates");
            System.exit(0);
        }

        /////////////////////////////////////////////////
        // OPTION PARSING
        if (line.hasOption("jar")) {

            String myjar = line.getOptionValue("jar");

            System.err.println(myjar);

//            long init = System.currentTimeMillis();
//            FileContainer fc = new FileContainer(file);
//            System.out.println(fc.getContent().length);
//            DHTStorageUnit dsu = new DHTStorageUnit(null, -1, (Serializable) fc);
//
//            community.save(fc.getName(), dsu, false);
//
//            long fin = System.currentTimeMillis();
//            System.out.println(file + " saved in " + (fin - init) + " ms");
//
//            System.gc();
            String rclass = "ozonecft.OzoneFilter";
            if (line.hasOption("runclass")) {
                rclass = line.getOptionValue("runclass");
            }

            ArrayList<String> toto = new ArrayList();

            if (line.hasOption("coords")) {
                String[] coordsargs = null;
                coordsargs = line.getOptionValue("coords").split("\\s+");
                if (coordsargs.length == 4) {
                    if (Float.parseFloat(coordsargs[0]) < Float.parseFloat(coordsargs[2]) && Float.parseFloat(coordsargs[1]) < Float.parseFloat(coordsargs[3])) {
                        toto.add(coordsargs[0]);
                        toto.add(coordsargs[1]);
                        toto.add(coordsargs[2]);
                        toto.add(coordsargs[3]);

                    }
                } else {
                    System.out.println("Option error:");
                    System.out.println(" -coords \"lllat lllon rulat rulon\"");
                    System.out.println("lllat - left lower latitude ");
                    System.out.println("lllon - left lower longitude ");
                    System.out.println("rulat - right upper latitude ");
                    System.out.println("lllat - left lower latitude ");

                }
            } else {
                toto.add("-89.5");
                toto.add("-179.5");
                toto.add("89.5");
                toto.add("179.5");
            }
            
            String steps="1";
            if (line.hasOption("step")) {
                steps = line.getOptionValue("step");
            }
            toto.add(steps);
            

            String datesargs[] = null;

            if (line.hasOption("dates")) {
                datesargs = line.getOptionValue("dates").split("\\s+");
                if (datesargs.length == 2) {
                    if (Integer.parseInt(datesargs[0]) < Integer.parseInt(datesargs[1])) {
                        toto.add(datesargs[0]);
                        toto.add(datesargs[1]);
                    }
                } else {
                    System.out.println("Option error:");
                    System.out.println(" -dates \"start end\"");
                    System.out.println("date format: YYYYMMDD");

                }
            }
            String per = "15";
            if (line.hasOption("period")) {
                per = line.getOptionValue("period");
            }
            toto.add(per);

            ArrayList<String> plainfiles = new ArrayList();

            if (line.hasOption("src")) {

                List<String> files = loadInput(line.getOptionValue("src"));

                System.err.println(files);
                System.err.println(files.size());

                Iterator it = files.iterator();
                int number = 0;
                while (it.hasNext()) {
                    String file = (String) it.next();
                    long init = System.currentTimeMillis();
                    FileContainer fc = new FileContainer(file);
                    DHTStorageUnit dsu = new DHTStorageUnit(null, -1, (Serializable) fc);

                    //((StorageAdapterInterface)P2P).blocking_save("input.data" + number, dsu, false);
                    community.save(dsu, fc.getName());

                    //save("input.data" + number, fc, false, number); 
                    // number++;
                    long fin = System.currentTimeMillis();

                    if (!fc.getName().endsWith(".jar")) {
                        plainfiles.add(fc.getName());
                        //toto.add(fc.getName());
                    }
                    System.err.println(fc.getName() + " (" + number + ") saved in " + (fin - init) + " ms");

                    //System.gc();
                }
                System.gc();

            }

            // les boucles
            // if we have more dates than the period 
            String[] params = toto.toArray(new String[toto.size()]);
            System.out.println(getDayCount(datesargs[0], datesargs[1]));
            if (getDayCount(datesargs[0], datesargs[1]) >= (Integer.parseInt(per))) {

                try {
                    GregorianCalendar gcal = new GregorianCalendar();
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

                    Date debut = sdf.parse(datesargs[0]);
                    Date fin = sdf.parse(datesargs[1]);
                    Date interval = addDays(fin, -1 * (Integer.parseInt(per)-1));

                    for (int i = 0; i < (plainfiles.size() - Integer.parseInt(per)); i++) {
                        ArrayList<String> days = new ArrayList<String>();

                        String currentfile = plainfiles.get(i);
                        currentfile.substring(0, currentfile.indexOf("."));
                        Date idate = sdf.parse(currentfile);

                        if (idate.compareTo(debut) >= 0 && idate.compareTo(interval) <= 0) {
                            for (int j = i; j < i + Integer.parseInt(per); j++) {
                                days.add(plainfiles.get(j));

                            }
                            String[] execdays = days.toArray(new String[days.size()]);
                            String[] pars = concatAll(params, execdays);
                            runApp(community, myjar, rclass, pars);
                        }

                    }

                } catch (java.text.ParseException ex) {
                    Logger.getLogger(Submitter.class.getName()).log(Level.SEVERE, null, ex);
                }
            } else {
                System.out.println("Not enough days of data to make a reliable evaluation");
                System.exit(0);
            }
        }

        System.gc();

        //job.setReducer("Reducer");
        //for (int i=0; i<repetitions; i++) {
        end = System.currentTimeMillis();

//                Thread.sleep(3000);
        System.out.println("Total time = " + (end - start));

        Scanner sc = new Scanner(System.in);
        String i = sc.next();
        //}

        System.exit(0);

    }

    private static void initNetwork(InetSocketAddress peer) {
        ///////////////////// Pastry

        /* Declaration of the main class
         * all the internal initialization is made on the constructor
         */
        TDTR = (CoreORB) TheBigFactory.getORB();


        /* Define if connecting to a peer or network discovery
         * 
         */
        CoreQueue queue = TheBigFactory.getCoreQueue();

        TDTR.setQueue(queue);

        /* creates a module to plug on the main class
         * and subscribe it to the messaging system
         */
        community = new Community(1, TDTR);

        //NetworkAdapterInterface P2P = new EasyPastryDHTAdapter(queue, peer, community);
        NetworkAdapterInterface P2P = new TomP2PAdapter(queue, peer, community);

        TDTR.setNetworkAdapter(P2P);

        TDTR.subscribe(community);

        //TDTR.setStorage(new SerializedDiskStorage());
        TDTR.setStorage((StorageAdapterInterface) P2P);

        System.err.println("starting network");

    }

    private static Serializable runApp(Community community, String jar, String app, String[] mapargs) {
        Number160 mapperId = null;// Identifiant de l'instance Mapper
        Serializable result = null;
        try {
            // ici on indique la classe qui fera le MAP
            mapperId = community.plug(jar, app, mapargs);
            System.err.println("mapperId = " + mapperId);
            result = community.waitJob(mapperId);
            
            ArrayList al = (ArrayList)result;
            for (int i = 0; i<al.size(); ++i)
            {
                System.out.println(al.get(i));
            }
            
            //community.removeJob(mapperId);
        } catch (Exception ex) {
            //Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        }
        return result;

    }

    private static void usage(Options options) {

        // Use the inbuilt formatter class
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Submitter", options);

    }

    /**
     * looks for input files on the arguments. If argument is a directory, it
     * includes all files inside, recursively.
     */
    private static List<String> loadInput(String dir) {
        if (filenames.isEmpty()) {
            File target = new File(dir);

            if (target.isDirectory()) {
                addDirectoryFiles(target);
            } else { // target is a file
                filenames.add(target.getPath());
            }
        }
        Collections.sort(filenames);
//        Collections.sort(filenames,new Comparator<String>() {
//        @Override
//        public int compare(String  fruite1, String  fruite2)
//        {
//            return  fruite1.compareTo(fruite2);
//        }
//    });
        return filenames;
    }

    private static boolean addDirectoryFiles(File target) {

        if (!target.isDirectory()) {
            filenames.add(target.getPath());
            return false;
        }

        File[] listOfFiles = target.listFiles();

        if (listOfFiles != null) {
            for (File file : listOfFiles) {
                addDirectoryFiles(file);
            }
        }
        return true;
    }

    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");

    public static long getDayCount(String start, String end) {
        long diff = -1;
        try {
            Date dateStart = simpleDateFormat.parse(start);
            Date dateEnd = simpleDateFormat.parse(end);

            //time is always 00:00:00 so rounding should help to ignore the missing hour when going from winter to summer time as well as the extra hour in the other direction
            diff = Math.round((dateEnd.getTime() - dateStart.getTime()) / (double) 86400000)+1; // +1 because the difference must be "inclusive" (15<->1=15)
        } catch (Exception e) {
            //handle the exception according to your own situation
        }
        return diff;
    }

    public static Date addDays(Date date, int days) {
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.add(cal.DATE, days);
        //minus number would decrement the days
        return cal.getTime();
    }

    public static String[] concatAll(String[]... jobs) {
        int len = 0;
        for (final String[] job : jobs) {
            len += job.length;
        }

        final String[] result = new String[len];

        int currentPos = 0;
        for (final String[] job : jobs) {
            System.arraycopy(job, 0, result, currentPos, job.length);
            currentPos += job.length;
        }

        return result;
    }

}
