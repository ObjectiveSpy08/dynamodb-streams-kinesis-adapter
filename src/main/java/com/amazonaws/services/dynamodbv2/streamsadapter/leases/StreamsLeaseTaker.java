package com.amazonaws.services.dynamodbv2.streamsadapter.leases;

import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.SentinelCheckpoint;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.impl.Lease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseTaker;
import com.amazonaws.services.kinesis.leases.interfaces.LeaseSelector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * DynamoDBStreams implementation of ILeaseTaker. Contains logic for taking and load-balancing leases.
 */
final class StreamsLeaseTaker<T extends Lease> implements ILeaseTaker<T> {

    private static final Log LOG = LogFactory.getLog(StreamsLeaseTaker.class);

    private static final int SCAN_RETRIES = 1;
    private static final int TAKE_RETRIES = 3;


    private static final Callable<Long> SYSTEM_CLOCK_CALLABLE = System::nanoTime;
    private final ILeaseManager<T> leaseManager;
    private final LeaseSelector<T> leaseSelector;
    private final String workerIdentifier;
    private final long leaseDurationNanos;
    private int maxLeasesForWorker = Integer.MAX_VALUE;
    private int maxLeasesToStealAtOneTime = 1;
    private final Map<String, T> allLeases = new HashMap<>();
    private long lastScanTimeNanos = 0L;

    public StreamsLeaseTaker(final ILeaseManager<T> leaseManager, final LeaseSelector<T> leaseSelector,
        final String workerIdentifier, final long leaseDurationMillis) {
        this.leaseManager = leaseManager;
        this.leaseSelector = leaseSelector;
        this.workerIdentifier = workerIdentifier;
        this.leaseDurationNanos = TimeUnit.MILLISECONDS.toNanos(leaseDurationMillis);
    }

    /**
     * Worker will not acquire more than the specified max number of leases even if there are more
     * shards that need to be processed. This can be used in scenarios where a worker is resource constrained or
     * to prevent lease thrashing when small number of workers pick up all leases for small amount of time during
     * deployment.
     * Note that setting a low value may cause data loss (e.g. if there aren't enough Workers to make progress on all
     * shards). When setting the value for this property, one must ensure enough workers are present to process
     * shards and should consider future resharding, child shards that may be blocked on parent shards, some workers
     * becoming unhealthy, etc.
     *
     * @param maxLeasesForWorker Max leases this Worker can handle at a time
     * @return LeaseTaker
     */
    public StreamsLeaseTaker<T> maxLeasesForWorker(final int maxLeasesForWorker) {
        if (maxLeasesForWorker <= 0) {
            throw new IllegalArgumentException("maxLeasesForWorker should be >= 1");
        }
        this.maxLeasesForWorker = maxLeasesForWorker;
        return this;
    }

    /**
     * Max leases to steal from a more loaded Worker at one time (for load balancing).
     * Setting this to a higher number can allow for faster load convergence (e.g. during deployments, cold starts),
     * but can cause higher churn in the system.
     *
     * @param maxLeasesToStealAtOneTime Steal up to this many leases at one time (for load balancing)
     * @return LeaseTaker
     */
    public StreamsLeaseTaker<T> maxLeasesToStealAtOneTime(final int maxLeasesToStealAtOneTime) {
        if (maxLeasesToStealAtOneTime <= 0) {
            throw new IllegalArgumentException("maxLeasesToStealAtOneTime should be >= 1");
        }
        this.maxLeasesToStealAtOneTime = maxLeasesToStealAtOneTime;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, T> takeLeases() throws DependencyException, InvalidStateException {
        return takeLeases(SYSTEM_CLOCK_CALLABLE);
    }

    /**
     * Internal implementation of takeLeases. Takes a callable that can provide the time to enable test cases without
     * Thread.sleep. Takes a callable instead of a raw time value because the time needs to be computed as-of
     * immediately after the scan.
     *
     * @param timeProvider Callable that will supply the time
     *
     * @return map of lease key to taken lease
     *
     * @throws DependencyException Thrown when there is a DynamoDb exception.
     * @throws InvalidStateException Represents error state.
     */
    synchronized Map<String, T> takeLeases(final Callable<Long> timeProvider)
        throws DependencyException, InvalidStateException {
        // Key is leaseKey
        Map<String, T> takenLeases = new HashMap<>();

        ProvisionedThroughputException lastException = null;

        for (int i = 1; i <= SCAN_RETRIES; i++) {
            try {
                updateAllLeases(timeProvider);
            } catch (ProvisionedThroughputException e) {
                LOG.info(String.format("Worker %s could not find expired leases on try %d out of %d",
                    workerIdentifier,
                    i,
                    TAKE_RETRIES));
                lastException = e;
            }
        }

        if (lastException != null) {
            LOG.error("Worker " + workerIdentifier
                    + " could not scan leases table, aborting takeLeases. Exception caught by last retry:",
                lastException);
            return takenLeases;
        }

        List<T> expiredLeases = getExpiredLeases();

        Set<T> leasesToTake;
        try {
            leasesToTake = computeLeasesToTake(expiredLeases);
        } catch (ProvisionedThroughputException e) {
            LOG.info(String.format("Worker %s could not compute leases to take due to capacity",
                workerIdentifier));
            return takenLeases;
        }
        Set<String> untakenLeaseKeys = new HashSet<>();

        for (T lease : leasesToTake) {
            String leaseKey = lease.getLeaseKey();

            for (int i = 0; i < TAKE_RETRIES; i++) {
                try {
                    if (leaseManager.takeLease(lease, workerIdentifier)) {
                        lease.setLastCounterIncrementNanos(System.nanoTime());
                        takenLeases.put(leaseKey, lease);
                    } else {
                        untakenLeaseKeys.add(leaseKey);
                    }
                    break;
                } catch (ProvisionedThroughputException e) {
                    LOG.info(String.format("Could not take lease with key %s for worker %s on try %d out of %d due to capacity",
                        leaseKey,
                        workerIdentifier,
                        i,
                        TAKE_RETRIES));
                }
            }
        }

        if (takenLeases.size() > 0) {
            LOG.info(String.format("Worker %s successfully took %d leases: %s",
                workerIdentifier,
                takenLeases.size(),
                stringJoin(takenLeases.keySet(), ", ")));
        }

        if (untakenLeaseKeys.size() > 0) {
            LOG.info(String.format("Worker %s failed to take %d leases: %s",
                workerIdentifier,
                untakenLeaseKeys.size(),
                stringJoin(untakenLeaseKeys, ", ")));
        }
        return takenLeases;
    }

    /** Package access for testing purposes.
     *
     * @param strings Collections of strings to be joined
     * @param delimiter Joining delimiter
     * @return Joined string.
     */
    static String stringJoin(final Collection<String> strings, final String delimiter) {
        StringBuilder builder = new StringBuilder();
        boolean needDelimiter = false;
        for (String string : strings) {
            if (needDelimiter) {
                builder.append(delimiter);
            }
            builder.append(string);
            needDelimiter = true;
        }

        return builder.toString();
    }

    /**
     * Scan all leases and update lastRenewalTime. Add new leases and delete old leases.
     *
     * @param timeProvider callable that supplies the current time
     * @throws ProvisionedThroughputException if listLeases fails due to lack of provisioned throughput
     * @throws InvalidStateException if the lease table does not exist
     * @throws DependencyException if listLeases fails in an unexpected way
     */
    private void updateAllLeases(Callable<Long> timeProvider)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {

        List<T> freshList = leaseManager.listLeases();
        try {
            lastScanTimeNanos = timeProvider.call();
        } catch (Exception e) {
            throw new DependencyException("Exception caught from timeProvider", e);
        }

        // This set will hold the lease keys not updated by the previous listLeases call.
        Set<String> notUpdated = new HashSet<>(allLeases.keySet());

        // Iterate over all leases, finding ones to try to acquire that haven't changed since the last iteration
        for (T lease : freshList) {
            String leaseKey = lease.getLeaseKey();
            long lastCounterIncrementNanos;
            T oldLease = allLeases.get(leaseKey);
            allLeases.put(leaseKey, lease);
            notUpdated.remove(leaseKey);
            // If we've seen this lease before..
            if (oldLease != null) {
                 // If the counter hasn't changed, propagate the lastCounterIncrementNanos time from the old lease
                 // else set it to the time of the scan
                lastCounterIncrementNanos = oldLease.getLeaseCounter().equals(lease.getLeaseCounter()) ?
                        oldLease.getLastCounterIncrementNanos() :
                        lastScanTimeNanos;
            } else {
                // If this new lease is unowned, it's never been renewed, else treat it as renewed as of the scan
                lastCounterIncrementNanos = lease.getLeaseOwner() == null ? 0L : lastScanTimeNanos;

                if (LOG.isDebugEnabled()) {
                    String debugMessage = lease.getLeaseOwner() == null ?
                        " as never renewed because it is new and unowned." :
                        " as recently renewed because it is new and owned.";

                    LOG.debug("Treating new lease with key " + leaseKey + debugMessage);
                }
            }
            lease.setLastCounterIncrementNanos(lastCounterIncrementNanos);
        }

        // Remove dead leases from allLeases
        for (String key : notUpdated) {
            allLeases.remove(key);
        }
    }

    /**
     * @return list of leases that were expired as of our last scan.
     */
    private List<T> getExpiredLeases() {
        return allLeases.values().stream()
            .filter(t -> t.isExpired(leaseDurationNanos, lastScanTimeNanos))
            .collect(Collectors.toList());
    }

    /**
     * @return List of all active shard leases that.
     */
    private List<T> getAllActiveLeases() {
        String shardEnd = SentinelCheckpoint.SHARD_END.toString();
        return allLeases.values().stream()
            .filter(lease -> lease instanceof KinesisClientLease)
            .filter(lease -> ((KinesisClientLease)lease).getCheckpoint() != null)
            .filter(lease -> !shardEnd.equals(((KinesisClientLease)lease).getCheckpoint()
                .getSequenceNumber()))
            .collect(Collectors.toList());
    }

    /**
     * Drops SHARD_END leases for the worker.
     * @param numLeasesToEvict Number of leases to evict.
     */
    private void evictShardEndLeases(int numLeasesToEvict) throws
        DependencyException,
        ProvisionedThroughputException,
        InvalidStateException {
        // KCL's Lease manager relies on side effects to update local copy of lease once it has done the eviction.
        // So we must iterate over allLeases.
        for (T lease : allLeases.values()) {
            if(numLeasesToEvict == 0) break;
            if(workerIdentifier.equals(lease.getLeaseOwner()) && lease instanceof KinesisClientLease) {
                KinesisClientLease kinesisClientLease = (KinesisClientLease) lease;
                if (kinesisClientLease.getCheckpoint() != null) {
                    String sequenceNumber = kinesisClientLease.getCheckpoint().getSequenceNumber();
                    if (SentinelCheckpoint.SHARD_END.toString().equals(sequenceNumber)) {
                        leaseManager.evictLease(lease);
                        numLeasesToEvict--;
                    }
                }
            }
        }
    }

    /**
     * Compute the number of leases I should try to take based on the state of the system.
     *
     * @param expiredLeases list of leases we determined to be expired
     * @return set of leases to take.
     */
    private Set<T> computeLeasesToTake(final List<T> expiredLeases) throws
        DependencyException,
        ProvisionedThroughputException,
        InvalidStateException {
        Map<String, Integer> leaseCountsByHost = computeLeaseCountsByHost(expiredLeases);
        Set<T> leasesToTake = new HashSet<>();
        List<T> allActiveLeases = getAllActiveLeases();

        int numAllLeases = allLeases.values().size();
        int numAllActiveLeases = allActiveLeases.size();
        int numWorkers = leaseCountsByHost.size();

        if (numAllLeases == 0) {
            // If there are no leases, I shouldn't try to take any.
            return leasesToTake;
        }

        int targetTotalLeases;
        int targetActiveLeases = 0;

        if (numWorkers >= numAllLeases) {
            // If we have n leases and n or more workers, each worker can have up to 1 lease, including myself.
            targetTotalLeases = 1;
        } else {
            /*
             * numWorkers must be < numAllLeases.
             *
             * Our target for each worker is numLeases / numWorkers (+1 if numWorkers doesn't evenly divide numLeases)
             */
            targetTotalLeases = numAllLeases / numWorkers + (numAllLeases % numWorkers == 0 ? 0 : 1);
            targetActiveLeases = numAllActiveLeases / numWorkers + (numAllActiveLeases % numWorkers == 0 ? 0 : 1);

            // Spill over is the number of leases this worker should have claimed, but did not because it would
            // exceed the max allowed for this worker.
            int leaseSpillover = Math.max(0, targetActiveLeases - maxLeasesForWorker);
            if (targetActiveLeases > maxLeasesForWorker) {
                // log exceptions
                LOG.warn(String.format("Worker %s target is %d active shard leases and maxLeasesForWorker is %d."
                        + " Resetting target to %d, lease spillover is %d. "
                        + " Note that some shards may not be processed if no other workers are able to pick them up resulting in a possible stall.",
                    workerIdentifier,
                    targetActiveLeases,
                    maxLeasesForWorker,
                    maxLeasesForWorker,
                    leaseSpillover));
                targetActiveLeases = maxLeasesForWorker;
            }
        }

        int myActiveLeaseCount = Math.toIntExact(allActiveLeases.stream()
            .filter(lease -> workerIdentifier.equals(lease.getLeaseOwner()))
            .count());

        int numTotalLeasesToReachTarget = targetTotalLeases - leaseCountsByHost.get(workerIdentifier);
        int numActiveLeasesToReachTarget = targetActiveLeases - myActiveLeaseCount;

        if(numActiveLeasesToReachTarget <= 0) return leasesToTake;
        // If we need more active leases than we have capacity for all leases, drop shard_end leases.
        if (numTotalLeasesToReachTarget < numActiveLeasesToReachTarget) {
            evictShardEndLeases(numActiveLeasesToReachTarget - numTotalLeasesToReachTarget);
        }

        // Shuffle expiredLeases so workers don't all try to contend for the same leases.
        Collections.shuffle(expiredLeases);

        int originalExpiredLeasesSize = expiredLeases.size();
        if (expiredLeases.size() > 0) {
            leasesToTake = leaseSelector.getLeasesToTakeFromExpiredLeases(expiredLeases, numActiveLeasesToReachTarget);
            originalExpiredLeasesSize = leasesToTake.size();
        } else {
            // If there are no expired leases and we need a lease, consider stealing.
            List<T> leasesToSteal = chooseLeasesToSteal(leaseCountsByHost, numActiveLeasesToReachTarget, targetActiveLeases, allActiveLeases);
            for (T leaseToSteal : leasesToSteal) {
                LOG.info(String.format(
                    "Worker %s needed %d leases but none were expired, so it will steal lease %s from %s",
                    workerIdentifier,
                    numActiveLeasesToReachTarget,
                    leaseToSteal.getLeaseKey(),
                    leaseToSteal.getLeaseOwner()));
                leasesToTake.add(leaseToSteal);
            }
        }

        if (!leasesToTake.isEmpty()) {
            LOG.info(String.format("Worker %s saw %d total leases, %d available active shard expired leases, %d " + "workers. Target " + "is %d leases, I have %d leases, I will take %d leases",
                workerIdentifier,
                numAllLeases,
                originalExpiredLeasesSize,
                numWorkers,
                targetActiveLeases,
                myActiveLeaseCount,
                leasesToTake.size()));
        }

        return leasesToTake;
    }

    /**
     * Choose leases to steal by randomly selecting one or more (up to max) from the most loaded worker.
     * Stealing rules:
     *
     * Steal up to maxLeasesToStealAtOneTime leases from the most loaded worker if
     * a) he has > target leases and I need >= 1 leases : steal min(leases needed, maxLeasesToStealAtOneTime)
     * b) he has == target leases and I need > 1 leases : steal 1
     *
     * @param leaseCounts map of workerIdentifier to lease count
     * @param needed # of leases needed to reach the target leases for the worker
     * @param target target # of leases per worker
     * @return Leases to steal, or empty list if we should not steal
     */
    private List<T> chooseLeasesToSteal(Map<String, Integer> leaseCounts, int needed, int target, List<T> activeLeases) {
        List<T> leasesToSteal = new ArrayList<>();

        Entry<String, Integer> mostLoadedWorker = null;
        // Find the most loaded worker
        for (Entry<String, Integer> worker : leaseCounts.entrySet()) {
            if (mostLoadedWorker == null || mostLoadedWorker.getValue() < worker.getValue()) {
                mostLoadedWorker = worker;
            }
        }

        int numLeasesToSteal = 0;
        if ((mostLoadedWorker.getValue() >= target) && (needed > 0)) {
            int leasesOverTarget = mostLoadedWorker.getValue() - target;
            numLeasesToSteal = Math.min(needed, leasesOverTarget);
            // steal 1 if we need > 1 and max loaded worker has target leases.
            if ((needed > 1) && (numLeasesToSteal == 0)) {
                numLeasesToSteal = 1;
            }
            numLeasesToSteal = Math.min(numLeasesToSteal, maxLeasesToStealAtOneTime);
        }

        if (numLeasesToSteal <= 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Worker %s not stealing from most loaded worker %s.  He has %d,"
                        + " target is %d, and I need %d",
                    workerIdentifier,
                    mostLoadedWorker.getKey(),
                    mostLoadedWorker.getValue(),
                    target,
                    needed));
            }
            return leasesToSteal;
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Worker %s will attempt to steal %d leases from most loaded worker %s. "
                        + " He has %d leases, target is %d, I need %d, maxLeasesToSteatAtOneTime is %d.",
                    workerIdentifier,
                    numLeasesToSteal,
                    mostLoadedWorker.getKey(),
                    mostLoadedWorker.getValue(),
                    target,
                    needed,
                    maxLeasesToStealAtOneTime));
            }
        }

        String mostLoadedWorkerIdentifier = mostLoadedWorker.getKey();
        List<T> candidates = activeLeases.stream()
            .filter(lease -> mostLoadedWorkerIdentifier.equals(lease.getLeaseOwner()))
            .collect(Collectors.toList());

        // Return random ones
        Collections.shuffle(candidates);
        int toIndex = Math.min(candidates.size(), numLeasesToSteal);
        leasesToSteal.addAll(candidates.subList(0, toIndex));

        return leasesToSteal;
    }

    /**
     * Count leases by host. Always includes myself, but otherwise only includes hosts that are currently holding
     * leases.
     *
     * @param expiredLeases list of leases that are currently expired
     * @return map of workerIdentifier to lease count
     */
    private Map<String, Integer> computeLeaseCountsByHost(List<T> expiredLeases) {
        Map<String, Integer> leaseCounts = new HashMap<>();
        // Compute the number of leases per worker by looking through allLeases and ignoring leases that have expired.
        allLeases.values().stream()
            .filter(lease -> !expiredLeases.contains(lease))
            .collect(Collectors.toList())
            .forEach(lease -> leaseCounts.merge(lease.getLeaseOwner(), 1, Integer::sum));

        // If I have no leases, I wasn't represented in leaseCounts. Let's fix that.
        leaseCounts.putIfAbsent(workerIdentifier, 0);
        return leaseCounts;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getWorkerIdentifier() {
        return workerIdentifier;
    }
}
