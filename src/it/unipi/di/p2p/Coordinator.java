package it.unipi.di.p2p;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;


/**
 * The Coordinator for the Chord overlay. It builds the overlay and
 * performs simulations on it.
 */
public class Coordinator {

    /**
     * Number of nodes in the overlay.
     */
    private int nodesNumber;
    /**
     * Number of bits to represent the identifiers.
     */
    private int idSpaceBits;
    /**
     * Maximum number of nodes in the ring.
     */
    private BigInteger idSpace;
    /**
     * Object to aggregate the simulations' results.
     */
    private AggregateResults ar = new AggregateResults();
    /**
     * Map to store the nodes, indexed by their ID
     */
    private SortedMap<BigInteger, Node> nodes;

    /**
     * Constructor of the class.
     * @param nodesNumber Number of nodes to place in the overlay.
     * @param idSpaceBits Number of bits to represent identifiers.
     */
    public Coordinator(int nodesNumber, int idSpaceBits) {
        this.nodesNumber = nodesNumber;
        this.idSpaceBits = idSpaceBits;
        idSpace = BigInteger.TWO.pow(idSpaceBits);
        nodes = new TreeMap<>();
    }

    /**
     * Builds the Chord overlay.
     *
     * The Coordinator generates {@code nodesNumber} strings of the form "IPaddress:port", where IP addressess
     * are generated as four random integers x.y.w.z between 0 and 255 and port is a random integer between
     * 0 and 65535. (The builder keeps track of generated strings and avoids duplicates.)
     *
     * Then, the string gets hashed and truncated to {@code idSpaceBits} bits and inserted into the ring, avoiding
     * duplicates.
     *
     * In any case, the builder throws an exception if at any point it takes more than 500k iterations to build a
     * single node.
     *
     * After creating the nodes, the builder proceeds to generate each node's finger table and to set each
     * node's successor and predecessor.
     *
     * @throws NoSuchAlgorithmException If the current JVM doesn't support SHA-512.
     */
    public void buildOverlay() throws NoSuchAlgorithmException {
        try {
            Random r = new Random();
            MessageDigest sha512 = MessageDigest.getInstance("SHA-512");
            Set<String> generated = new HashSet<>();
            int iterations = 0;

            for (int i = 0; i < nodesNumber; i++) {
                // Generate the string
                String ipAddr = String.format("%d.%d.%d.%d",
                        r.nextInt(256), r.nextInt(256),
                        r.nextInt(256), r.nextInt(256));
                InetAddress addr = InetAddress.getByName(ipAddr);
                int port = r.nextInt(65536);
                String iport = addr.getHostAddress() + ":" + port;
                if (generated.contains(iport)) {
                    // If the current string has already been generated, retry
                    i--;
                    iterations++;
                }
                else {
                    // Generate the hash and truncate it
                    byte[] sha512Byte = sha512.digest(iport.getBytes());
                    byte[] id = Util.truncate(sha512Byte, idSpaceBits);
                    BigInteger curr = new BigInteger(1, id);
                    if (nodes.containsKey(curr)) {
                        i--;
                        if (iterations == 500000) {
                            throw new RuntimeException("Too many collisions in map!");
                        } else {
                            iterations++;
                        }
                    } else {
                        iterations = 0;
                        nodes.put(curr, new Node(this, idSpaceBits, addr, port, curr));
                        generated.add(iport);
                    }
                }
            }

        } catch (UnknownHostException e) {
            // Should never occur, since IP addresses are built correctly
            throw new AssertionError(e);
        }

        Set<BigInteger> keys = nodes.keySet();

        Node prev = nodes.get(nodes.lastKey());
        int count = 0;
        for(BigInteger id: keys) {
            // Calculate the [prev, curr) interval's length for statistics purposes
            BigInteger interval = id.subtract(prev.getId());
            if (count == 0)
                // The first time, the interval is negative since the ring wraps around
                // Add 2^(idSpaceBits) to compensate
                interval = interval.add(idSpace);
            ar.addDistance(interval);
            Node n = nodes.get(id);
            prev.setSuccessor(n);
            n.setPredecessor(prev);
            prev = n;
            count++;
            System.out.println("Generating fingertable #" + count);
            BigInteger[] ftab = createFingerTable(id);
            n.setFingertable(ftab);
        }
    }

    /**
     * Creates the finger table for a particular ID.
     *
     * The built finger tables employ a trick to save space for the data structure: if for any x it holds
     * {@code fingertable[x] == fingertable[0] == successor(ID)}, then a null value is stored instead
     * of the actual value. In this way, the algorithm that uses the finger table ({@link Node}'s {@code lookup} method)
     * has to substitute any null value with {@code fingertable[0]}.
     * @param id the ID of the node for which the finger table must be built
     * @return an array of {@link BigInteger}s corresponding to the node's finger table
     */
    private BigInteger[] createFingerTable(BigInteger id) {
        BigInteger[] fTable = new BigInteger[idSpaceBits];
        BigInteger successor = null;
        for (int i = 0; i < idSpaceBits; i++) {
            BigInteger pointed = BigInteger.TWO.pow(i).add(id).mod(idSpace);
            BigInteger found = findNearestNode(pointed);
            if (0 == i) successor = found;
            else if (found.equals(successor)) {
                found = null;
            }
            fTable[i] = found;
        }

        return fTable;
    }

    /**
     * Given an ID, returns the ID of the first node n such that {@code n.getID() >= ID}.
     * @param id The ID to be searched.
     * @return The ID of the first node that satisfies the above property.
     */
    private BigInteger findNearestNode(BigInteger id) {
        try {
            return nodes.tailMap(id).firstKey();
        } catch (NoSuchElementException e) {
            return nodes.firstKey();
        }
    }

    /**
     * Given an ID, returns the corresponding {@link Node} (or {@code null} if no Node corresponds to
     * that ID).
     *
     * @param b The ID of the desired node.
     * @return The desired {@link Node}, or null.
     */
    public Node getNode(BigInteger b) {
        return nodes.get(b);
    }

    /**
     * Represents the overlay in a Comma-Separate Values (CSV) format.
     *
     * @return a {@link String} representing the overlay in CSV format.
     */
    public String getTopology() {
        StringBuilder sb = new StringBuilder();

        for (BigInteger id: nodes.keySet()) {
            sb.append(nodes.get(id).toCSV());
        }

        return sb.toString();
    }

    /**
     * Performs a simulation of a certain number of queries, as if they were done by different nodes.
     * Each query is logged for statistics purposes.
     *
     * @param number The number of queries to be performed.
     * @return a {@link String} containing statistics of the simulation.
     * @throws NoSuchAlgorithmException If the current JVM doesn't support SHA-512.
     */
    public String simulateRouting(int number) throws NoSuchAlgorithmException {
        MessageDigest sha512 = MessageDigest.getInstance("SHA-512");
        Random r = new Random();

        // Take a list of nodes randomly shuffled
        ArrayList<BigInteger> integers = new ArrayList<>(nodes.keySet());
        Collections.shuffle(integers);


        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < number; i++) {
            // If the list is empty, refill it
            if (integers.isEmpty()) {
                integers.addAll(nodes.keySet());
                Collections.shuffle(integers);
            }
            // The ID to be searched is generated as a random byte array of size idSpaceBits
            byte[] random = new byte[idSpaceBits];
            r.nextBytes(random);
            System.out.println("Generated search key: " + Util.bytesToHex(random));
            // Then, it gets hashed and truncated
            BigInteger toSearch = new BigInteger(1, Util.truncate(sha512.digest(random), idSpaceBits));
            // Elect a random node to be the one performing the query
            Node n = nodes.get(integers.remove(0));
            RouteLogger rl = new RouteLogger(toSearch, n.getId());
            System.out.println("+++++++++++++ Starting searching for " + Util.bytesToHex(toSearch.toByteArray()) +
                               " from node " + n.getReadableName() + " (" + Util.bytesToHex(n.getId().toByteArray())+ ")");
            n.lookup(toSearch, rl);
            System.out.println("+++++++++++++ Search ended for " + Util.bytesToHex(toSearch.toByteArray()) + " no. hops: " + rl.getNumberOfHops());
            // Log useful statistics
            ar.addMultipleQueries(rl.getHops());
            ar.addHopCounts(rl.getNumberOfHops());
            ar.addEndNodeCount(Util.bytesToHex(rl.getEndNode().toByteArray()));
            ar.addSingleQuery(rl.getEndNode());
        }

        sb.append(ar.toCSV());
        return sb.toString();
    }

    /**
     * A Class to hold aggregate statistics on the ran simulations.
     */
    private class AggregateResults {
        /**
         * Stores the number of queries that each node performs.
         *
         * (A node performs a query if its {@code lookup} method is called.)
         */
        Map<BigInteger, Integer> queriesReceivedByEachNode = new HashMap<>();
        /**
         * Stores the number of nodes that perform a certain number of queries,
         * i.e. if there are x nodes that perform y queries (each), then the
         * pair (y, x) is stored in this map.
         */
        Map<Integer, Integer> nodesForEachQueryNumber = new HashMap<>();
        /**
         * Stores the number of occurrences of a certain distance between
         * any node and its predecessor.
         */
        Map<BigInteger, Integer> distances = new HashMap<>();
        /**
         * Stores the number of occurrences of queries of a certain length.
         */
        Map<Integer, Integer> hopCounts = new HashMap<>();
        /**
         * Stores a list of nodes that are endnodes for some query (and the
         * number of queries for which that particular node is endnode).
         */
        Map<String, Integer> endnodes = new HashMap<>();
        /**
         * Counts the distinct endnodes
         */
        int endNodeCount = 0;
        /**
         * Standard deviation for the distance between consecutive nodes.
         */
        double stdDevDist = 0.0;
        /**
         * Average distance (number of identifiers) between consecutive nodes.
         */
        double avgDist = 0.0;
        /**
         * Standard deviation of the number of hops for a query.
         */
        double stdDevHops = 0.0;
        /**
         * Average number of hops for a query.
         */
        double avgHops = 0.0;
        /**
         * Average number of queries that are performed by each node.
         */
        double avgQueriesPerNode = 0.0;

        /**
         * Adds the distance between two nodes and updates the average and standard deviation.
         *
         * @param dist The distance to add to the statistics.
         */
        void addDistance(BigInteger dist) {
            update(distances, dist);
            BigDecimal sum = BigDecimal.ZERO, size = BigDecimal.ZERO, stdevsum = BigDecimal.ZERO;
            for (BigInteger hop: distances.keySet()) {
                int count = distances.get(hop);
                BigDecimal bd = new BigDecimal(String.format("%d", count));
                sum = sum.add(bd.multiply(new BigDecimal(hop)));
                size = size.add(bd);
            }
            BigDecimal avgDistBD = sum.divide(size, RoundingMode.HALF_DOWN);
            avgDist = avgDistBD.doubleValue();
            for (BigInteger hop: distances.keySet()) {
                int count = distances.get(hop);
                BigDecimal bd = new BigDecimal(String.format("%d", count));
                stdevsum = stdevsum.add(bd.multiply(new BigDecimal(hop).subtract(avgDistBD).pow(2)));
            }
            stdDevDist = stdevsum.divide(size, RoundingMode.HALF_DOWN).sqrt(MathContext.DECIMAL32).doubleValue();
        }

        /**
         * Adds an endnode to the statistics.
         *
         * @param s The endnode to add to the statistics.
         */
        void addEndNodeCount(String s) {
            update(endnodes, s);
            endNodeCount = endnodes.size();
        }

        /**
         * Adds a single query to the statistics, updating also the average queries per node.
         *
         * @param query The query to add to the statistics.
         */
        void addSingleQuery(BigInteger query) {
            update(queriesReceivedByEachNode, query);
            update(nodesForEachQueryNumber, queriesReceivedByEachNode.get(query));
            avgQueriesPerNode = average(nodesForEachQueryNumber);
        }

        /**
         * Add a sequence of queries to the statistics, updating also the average queries per node.
         *
         * @param qc And {@link ArrayList} of queries to add to the statistics.
         */
        void addMultipleQueries(ArrayList<BigInteger> qc) {
            for (BigInteger step: qc) {
                update(queriesReceivedByEachNode, step);
                update(nodesForEachQueryNumber, queriesReceivedByEachNode.get(step));
            }
            avgQueriesPerNode = average(nodesForEachQueryNumber);
        }

        /**
         * Adds the number of hops of a query to the statistics, updating the relevant average and standard deviation.
         *
         * @param hops Number of hops of the current query
         */
        void addHopCounts(int hops) {
            update(hopCounts, hops);
            avgHops = average(hopCounts);
            stdDevHops = stdDeviation(hopCounts, avgHops);
        }

        /**
         * Outputs the current statistics in CSV format.
         *
         * @return A {@link String} containing the statistics collected insofar, in CSV format.
         */
        String toCSV() {
            StringBuilder sb = new StringBuilder();

            sb
                    .append("avg_queries_per_node,").append(avgQueriesPerNode).append('\n')
                    .append("end_nodes,").append(endNodeCount).append('\n')
                    .append("average_distance,").append(avgDist).append('\n')
                    .append("std_dev_distance,").append(stdDevDist).append('\n')
                    .append("avg_hops_per_query,").append(avgHops).append('\n')
                    .append("std_dev_hops_per_query,").append(stdDevHops).append('\n')
                    .append('\n').append("distance,count").append('\n');

            for (BigInteger distance : distances.keySet()) {
                sb.append(distance).append(',').append(distances.get(distance)).append('\n');
            }

            sb.append('\n').append("query_number,nodes").append('\n');

            for (Integer query: nodesForEachQueryNumber.keySet()) {
                sb.append(query).append(',').append(nodesForEachQueryNumber.get(query)).append('\n');
            }

            sb.append('\n').append("hops_per_query,times").append('\n');

            for (Integer query: hopCounts.keySet()) {
                sb.append(query).append(',').append(hopCounts.get(query)).append('\n');
            }

            return sb.toString();
        }

        /**
         * Computes the average of a map where the keys are the elements
         * and the values are their occurrences' count.
         *
         * @param m The {@link Map} that stores the data to average.
         * @return The average of the given data.
         */
        private double average(Map<Integer, Integer> m) {
            int sum = 0, size = 0;
            for (int hop: m.keySet()) {
                int count = m.get(hop);
                sum += hop * count;
                size += count;
            }
            return ((double) sum)/size;
        }

        /**
         * Computes the standard deviation of a map where the keys are the elements
         * and the values are their occurrences' count.
         *
         * @param m The {@link Map} that stores the data to find the standard deviation.
         * @param avg The average for the abovementioned map.
         * @return The standard deviation of the given data.
         */
        private double stdDeviation(Map<Integer, Integer> m, double avg) {
            double sum = 0.0, size = 0.0;
            for (int hop: m.keySet()) {
                int count = m.get(hop);
                sum += (hop - avg) * (hop - avg) * count;
                size += count;
            }
            return Math.sqrt(sum/size);
        }

        /**
         * Updates one statistics with the received data.
         *
         * @param m the Map containing the data for the statistics.
         * @param key The newly-received data.
         * @param <T> The type of the data.
         */
        private <T> void update(Map<T, Integer> m, T key) {
            if (m.containsKey(key)) {
                int count = m.get(key);
                count++;
                m.put(key, count);
            } else {
                m.put(key, 1);
            }
        }

    }

}
