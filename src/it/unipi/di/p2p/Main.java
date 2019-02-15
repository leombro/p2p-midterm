package it.unipi.di.p2p;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Main executable class.
 */
public class Main {
    public static void main(String[] args) throws NoSuchAlgorithmException {

        if (args.length < 2 || Integer.parseInt(args[0]) % 4 != 0) {
            System.out.println("Invalid invocation, please provide identifiers' bitsize " +
                    "(divisible by 4) and number of nodes");
        } else {
            int idSize = Integer.parseInt(args[0]), nodesNumber = Integer.parseInt(args[1]);
            BigInteger ids = BigInteger.TWO.pow(idSize), nodes = BigInteger.valueOf(nodesNumber);
            if (idSize > 512) {
                System.out.println("Please provide an identifier size of no more than 512 bits.");
            } else if (nodes.compareTo(ids) > 0) {
                System.out.println("Too many nodes requested");
            } else {
                final String prefix = idSize + "bit_";
                final String extension = ".csv";
                final String topology = "topologies/" + nodesNumber + "/";
                final String routing = "routing/" + nodesNumber + "/";

                Coordinator c = new Coordinator(nodesNumber, idSize);

                c.buildOverlay();

                FileWriter fw = null, fw1 = null;
                PrintWriter pw = null, pw1 = null;

                String filename = prefix + LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM_HHmmss"));

                // Creates two files: ./topologies/$nodesNumber/$idSize_$currentTime.csv
                // and ./routing/$nodesNumber/$idSize_$currentTime.csv
                try {
                    Files.createDirectories(Paths.get(topology));
                    Files.createDirectories(Paths.get(routing));

                    fw = new FileWriter(topology + filename + extension);
                    pw = new PrintWriter(new BufferedWriter(fw));

                    fw1 = new FileWriter(routing + filename + extension);
                    pw1 = new PrintWriter(new BufferedWriter(fw1));

                    pw.print(c.getTopology());
                    pw1.print(c.simulateRouting(nodesNumber));

                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (pw != null) {
                            pw.close();
                        }
                        if (fw != null) {
                            fw.close();
                        }
                        if (pw1 != null) {
                            pw1.close();
                        }
                        if (fw1 != null) {
                            fw1.close();
                        }
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
    }
}
