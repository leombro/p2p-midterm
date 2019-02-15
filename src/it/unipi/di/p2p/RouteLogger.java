package it.unipi.di.p2p;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.math.BigInteger;
import java.util.ArrayList;

/**
 * Objects of class RouteLogger get passed along nodes of a query and
 * register useful metrics to calculate statistics.
 */

public class RouteLogger {


    /**
     * Hash of the key to be found in the query.
     */
    private String toFind;
    /**
     * List of hops of the query.
     */
    private ArrayList<BigInteger> hops;
    /**
     * Number of hops of the query.
     */
    private int no_hops;
    /**
     * Hash of the last Node of the query, the one that satisfies the request.
     */
    private BigInteger endNode = null;
    /**
     * Hash of the first Node of the query, the one that sends the request.
     */
    private BigInteger startNode;

    /**
     * Constructor of the class.
     *
     * @param toFind Hash of the key to be found in the query
     * @param startNode Hash of the first Node of the query
     */
    public RouteLogger(BigInteger toFind, BigInteger startNode) {
        this.toFind = Util.bytesToHex(toFind.toByteArray());
        this.startNode = startNode;
        hops = new ArrayList<>();
    }

    /**
     * Adds a single hop to the list.
     * @param currNode The node to be added to the hops' list
     */
    public void addHop(BigInteger currNode) {
        hops.add(currNode);
    }

    /**
     * Adds the exit node for the current query.
     * @param endNode The node that satisfies the query
     */
    public void addEndNode(BigInteger endNode) {
        this.endNode = endNode;
    }

    /**
     * Returns the (current) number of hops of the query.
     * @return the number of hops of the query.
     */
    public int getNumberOfHops() {
        return hops.size();
    }

    /**
     * Returns the list of hops of the query.
     * @return the list of hops of the query.
     */
    public ArrayList<BigInteger> getHops() {
        return hops;
    }

    /**
     * Returns the hash of the final node of the query.
     * @return the hash of the final node of the query.
     */
    public BigInteger getEndNode() {
        return endNode;
    }

    /**
     * Returns a JSON representation for this object.
     *
     * @return a String containing the JSON representation of this object.
     */
    public String getJSON() {
        no_hops = hops.size();
        Gson g = new GsonBuilder().setPrettyPrinting().create();
        return g.toJson(this);
    }
}
