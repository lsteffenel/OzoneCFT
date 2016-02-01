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
import cloudfit.storage.StorageAdapterInterface;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Scanner;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 *
 * @author Luiz Angelo STEFFENEL <Luiz-Angelo.Steffenel@univ-reims.fr>
 */
public class Worker {

    private static Community community;
    private static CoreORB TDTR;

    public static void main(String[] args) {
        long start;
        long end;

        Options options = new Options();
        Option help = new Option("help", "print this message");
        Option node = OptionBuilder.withArgName("node")
                .hasArg()
                .withDescription("Optional address to join the P2P network")
                .create("node");
        Option port = OptionBuilder.withArgName("port")
                .hasArg()
                .withDescription("Optional port to join the P2P network")
                .create("port");

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
        
        
        
        //TomP2PAdapter P2P = new TomP2PAdapter(null, peer, null);

        start = System.currentTimeMillis();

       
        

        //job.setReducer("Reducer");
        //for (int i=0; i<repetitions; i++) {
        end = System.currentTimeMillis();

//                Thread.sleep(3000);
        System.err.println(
                "Total time = " + (end - start));

        System.out.println(
                "Total time = " + (end - start));

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

        System.out.println("starting network");

    }
    
    

    private static void usage(Options options) {

        // Use the inbuilt formatter class
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Worker", options);

    }

}
