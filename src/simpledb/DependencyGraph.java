package simpledb;

import java.util.AbstractMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.LinkedList;

public class DependencyGraph {
    private HashMap<TransactionId, HashSet<AbstractMap.SimpleEntry<PageId, TransactionId>>> blockingMe; // waiting tids are the keys
    private HashMap<TransactionId, HashSet<AbstractMap.SimpleEntry<PageId, TransactionId>>> waitingOnMe;

    public DependencyGraph() {
        blockingMe = new  HashMap<TransactionId, HashSet<AbstractMap.SimpleEntry<PageId, TransactionId>>>();
        waitingOnMe = new HashMap<TransactionId, HashSet<AbstractMap.SimpleEntry<PageId, TransactionId>>>();
    }

    public synchronized void addDependency(TransactionId waiter, TransactionId runner, PageId pid) throws TransactionAbortedException {
        if(waiter.equals(runner)) // block self-looping dependencies
            return;

        // add runner to graph in order to properly keep track of graph size
        if(!blockingMe.containsKey(runner))
            blockingMe.put(runner, new HashSet<AbstractMap.SimpleEntry<PageId, TransactionId>>());

        // update incoming and outgoing edges in dependency graph
        AbstractMap.SimpleEntry<PageId, TransactionId> newBlockingMeValue = new AbstractMap.SimpleEntry<PageId, TransactionId>(pid, runner);
        if(!blockingMe.containsKey(waiter))
            blockingMe.put(waiter, new HashSet<AbstractMap.SimpleEntry<PageId, TransactionId>>());
        blockingMe.get(waiter).add(newBlockingMeValue);

        if(!waitingOnMe.containsKey(runner))
            waitingOnMe.put(runner, new HashSet<AbstractMap.SimpleEntry<PageId, TransactionId>>());
        AbstractMap.SimpleEntry<PageId, TransactionId> newWaitingOnMeValue = new AbstractMap.SimpleEntry<PageId, TransactionId>(pid, waiter);
        waitingOnMe.get(runner).add(newWaitingOnMeValue);

        if(detectDeadlock()) {
            _removeDependency(waiter, runner, pid);
            throw new TransactionAbortedException();
        }
    }

    private  void _removeDependency(TransactionId finishedRunning, TransactionId waiting, PageId pid) {
        if(!blockingMe.containsKey(waiting))
            return;
        HashSet<AbstractMap.SimpleEntry<PageId, TransactionId>> waitingOn = blockingMe.get(waiting);
        AbstractMap.SimpleEntry<PageId, TransactionId> completed = new AbstractMap.SimpleEntry<PageId, TransactionId>(pid, finishedRunning);
        waitingOn.remove(completed);
        AbstractMap.SimpleEntry<PageId, TransactionId> waitingOnMeValueToDelete = new AbstractMap.SimpleEntry<PageId, TransactionId>(pid, waiting);
        waitingOnMe.get(finishedRunning).remove(waitingOnMeValueToDelete);
    }

    public synchronized void removeDependencies(TransactionId finishedRunning, PageId pid) {
        if(!waitingOnMe.containsKey(finishedRunning))
            return;
        HashSet<AbstractMap.SimpleEntry<PageId, TransactionId>> blockedByMe = (HashSet<AbstractMap.SimpleEntry<PageId, TransactionId>>) waitingOnMe.get(finishedRunning).clone();
        for(AbstractMap.SimpleEntry<PageId,TransactionId> blocked : blockedByMe) {
            if(blocked.getKey().equals(pid))
                _removeDependency(finishedRunning, blocked.getValue(), pid);
        }
    }

    // use topological sort to detect cycles in this disconnected DAG with parallel edges
    private boolean detectDeadlock() {

        // calculate in-degrees
        HashMap<TransactionId, Integer> indegrees = new HashMap<TransactionId, Integer>();
        for(Map.Entry<TransactionId, HashSet<AbstractMap.SimpleEntry<PageId, TransactionId>>> entry : blockingMe.entrySet()) {
            TransactionId source = entry.getKey();
            if(!indegrees.containsKey(source))
                indegrees.put(source, 0);

            HashSet<AbstractMap.SimpleEntry<PageId, TransactionId>> destinations = entry.getValue();
            for(AbstractMap.SimpleEntry<PageId, TransactionId> p : destinations) {
                TransactionId dest = p.getValue();
                if(!indegrees.containsKey(dest))
                    indegrees.put(dest, 0);
                indegrees.put(dest, indegrees.get(dest)+1);
            }
        }

        // find sources
        Queue<TransactionId> sources = new LinkedList<TransactionId>();
        for(Map.Entry<TransactionId, Integer> entry : indegrees.entrySet()) {
            TransactionId currSource = entry.getKey();
            int indegree = entry.getValue();
            if(indegree == 0) {
                int outdegree = blockingMe.get(currSource).size();
                if(outdegree == 0)
                    blockingMe.remove(currSource); // remove nodes with zero indegree and outdegree
                else
                    sources.add(entry.getKey());
            }
        }

        int visitedCount = 0;
        // decrement in-degrees while removing sources
        while(sources.size() > 0) {
            TransactionId current = sources.poll();
            visitedCount++;
            HashSet<AbstractMap.SimpleEntry<PageId, TransactionId>> children = blockingMe.get(current);
            for(AbstractMap.SimpleEntry<PageId, TransactionId> p : children) {
                TransactionId childId = p.getValue();
                indegrees.put(childId, indegrees.get(childId)-1);
                if(indegrees.get(childId) == 0)
                    sources.add(childId);
            }
        }

        return visitedCount != blockingMe.size();
    }
}