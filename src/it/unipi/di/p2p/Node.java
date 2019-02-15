package it.unipi.di.p2p;

import java.math.BigInteger;
import java.net.InetAddress;

/**
 * A class representing the nodes of the Chord overlay.
 */
public class Node {

    /**
     * A private inner class that represents the network address of a node.
     */
    private class NodeAddress {
        InetAddress addr;
        int port;

        /**
         * Constructor for the NodeAddress class.
         * @param addr The IP address of the node.
         * @param port The port exposed by the node.
         */
        NodeAddress(InetAddress addr, int port) {
            this.addr = addr;
            this.port = port;
        }

        @Override
        public String toString() {
            return addr.getHostAddress() + ":" + port;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o == null || !(o instanceof NodeAddress)) {
                return false;
            }

            NodeAddress n = (NodeAddress) o;
            return n.addr.equals(addr) && n.port == port;
        }
    }

    /**
     * The number of bits for representing identifiers.
     */
    private int idSpace;
    /**
     * The node's finger table.
     */
    private BigInteger[] fingertable;
    /**
     * The NodeAddress representing this Node's IP address and port.
     */
    private NodeAddress nAddr;
    /**
     * A reference to the overlay's coordinator.
     */
    private Coordinator coordinator;
    /**
     * This node's id.
     */
    private BigInteger id;
    /**
     * This node's predecessor.
     */
    private Node predecessor;
    /**
     * This node's successor.
     */
    private Node successor;

    /**
     * The Node's constructor.
     *
     * @param coordinator A reference to the overlay's coordinator.
     * @param idSpace The number of bits for representing identifiers.
     * @param addr The IP address of the node.
     * @param port The port exposed by the node.
     * @param id The id assigned to this node.
     */
    public Node(Coordinator coordinator, int idSpace, InetAddress addr, int port, BigInteger id) {
        this.coordinator = coordinator;
        this.idSpace = idSpace;
        this.fingertable = null;
        this.nAddr = new NodeAddress(addr, port);
        this.id = id;
        this.predecessor = null;
        this.successor = null;
    }

    /**
     * Checks whether a certain key is contained in this node. The method is always called in the "right"
     * node, in the sense that the node that calls this method is surely the one delegated to store the data (i.e.
     * the data's ID falls into the interval (prev, this]).
     *
     * This is a dummy method that always returns true, since there's no storing of the data in this simulation.
     *
     * @param dataID The ID of the data to be found
     * @param logger A {@link RouteLogger} object to collect statistics.
     * @return Always true.
     */
    public boolean contains(BigInteger dataID, RouteLogger logger) {
        logger.addEndNode(this.id);
        return true;
    }

    /**
     * Sets this node's successor.
     * @param successor The node to be set as this node's successor.
     */
    public void setSuccessor(Node successor) {
        this.successor = successor;
    }

    /**
     * Sets this node's predecessor.
     * @param predecessor The node to be set as this node's predecessor.
     */
    public void setPredecessor(Node predecessor) {
        this.predecessor = predecessor;
    }

    /**
     * Performs the a query to search for some data.
     * The lookup algorithm is the standard one found in Chord's specification paper.
     *
     * @param dataID The ID of the data to search.
     * @param logger A {@link RouteLogger} object to collect statistics
     * @return true if the lookup succeeds.
     */
    public boolean lookup(BigInteger dataID, RouteLogger logger) {

        BigInteger wrapPoint = BigInteger.TWO.pow(idSpace);

        if (Util.isInInterval(true, dataID, predecessor.id, id, wrapPoint)) {
            return this.contains(dataID, logger);
        } else if (Util.isInInterval(true, dataID, id, successor.id, wrapPoint)) {
            logger.addHop(id);
            return successor.contains(dataID, logger);
        } else {
            logger.addHop(id);
            Node n = closestPrecedingNode(dataID);
            if (n.equals(this)) return this.contains(dataID, logger);
            return n.lookup(dataID, logger);
        }
    }

    /**
     * A method to find the closest preceding node for any ID value on the ring.
     *
     * @param dataID The ID of the data to search.
     * @return the closes preceding node for the given ID.
     */
    private Node closestPrecedingNode(BigInteger dataID) {
        for (int i = idSpace - 1; i >= 0; i--) {
            BigInteger curr = extract(fingertable[i]);
            if (Util.isInInterval(false, curr, id, dataID, BigInteger.TWO.pow(idSpace))) {
                return coordinator.getNode(curr);
            }
        }
        return successor;
    }

    /**
     * Sets this node's finger table.
     * @param fingertable An array of @{@link BigInteger} to be set as this node's finger table.
     */
    public void setFingertable(BigInteger[] fingertable) {
        this.fingertable = fingertable;
    }

    /**
     * Gets this node's ID.
     * @return The ID for this node.
     */
    public BigInteger getId() {
        return id;
    }

    /**
     * A method to return the correct ID for a finger of this node. See @{@link Coordinator}.createFingerTable method.
     * @param which An ID to be checked.
     * @return The same ID or this node's successor, if {@code which == null}.
     */
    private BigInteger extract(BigInteger which) {
        return (which == null) ? fingertable[0] : which;
    }

    /**
     * Gets the node's address in a readable, colon-separated string.
     * @return a {@link String} containing this node's address in a readable format.
     */
    public String getReadableName() {
        return nAddr.toString();
    }

    /**
     * Gets the node's finger table in a Comma-Separated Values (CSV) format.
     *
     * @return a {@link String} containing this node's finger table in CSV format.
     */
    public String toCSV() {
        StringBuilder sb = new StringBuilder();
        String thisName = Util.bytesToHex_NoTrim(this.id.toByteArray());//.getReadableName();

        for (int i = 0; i < idSpace; i++) {
            sb.append(thisName)
                    .append(",")
                    .append(Util.bytesToHex_NoTrim(coordinator.getNode(extract(fingertable[i])).getId().toByteArray()))//ReadableName())
                    .append('\n');
        }

        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null || !(o instanceof Node)) {
            return false;
        }

        Node n = (Node) o;
        return n.id.equals(id) && n.nAddr.equals(nAddr);
    }

}
