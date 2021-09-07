using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using Scheduler;
using Serilog;
using SharpScheduler = Scheduler.Scheduler;

namespace ChordDHT
{
    public sealed class ChordServer : IDisposable
    {
        public Node LocalNode { get; }
        public Node SeedNode { get; private set; }

        /// <summary>
        /// The FingerTable contains reasonably up-to-date successor ChordNode owners for exponentially
        /// distant ID values.  The FingerTable is maintained in the background by the maintenance
        /// processes and is used in navigation as a shortcut when possible.
        /// </summary>
        public FingerTable FingerTable { get; }
        public BigInteger Id => LocalNode.Id;

        /// <summary>
        /// The SuccessorCache is used to keep the N (N == SuccessorCache.Length) closest successors
        /// to this ChordInstance handy.  Different values for the size of the SuccessorCache length
        /// can impact performance under churn and in varying (often, smaller) sized Chord rings.
        /// </summary>
        public Node[] Successors { get; }

        /// <summary>
        /// The Successor is the ChordNode that follows the current ChordInstance in the Chord ring.
        /// Since the Successor is also the first item in the SuccessorCache, we simply get/set out of the
        /// first item stored in the SuccessorCache.
        /// </summary>
        public Node Successor
        {
            get => Successors[0];
            private set
            {
                if (value == null || string.IsNullOrEmpty(value.Host) || value.Port == 0)
                {
                    Debugger.Break();
                }
                Successors[0] = value;
            }
        }

        /// <summary>
        /// The Predecessor is the ChordNode that precedes the current ChordInstance in the Chord ring.
        /// </summary>
        public Node? Predecessor { get; private set; }

        private readonly IScheduler _scheduler;
        private IDisposable _maintenanceTask = Disposable.Empty;

        public ChordServer(Node localNode)
            : this(localNode, new[] { localNode, localNode, localNode }, new SharpScheduler())
        {
        }

        public ChordServer(Node localNode, Node[] successors, IScheduler scheduler)
        {
            _scheduler = scheduler;
            LocalNode = localNode;
            SeedNode = localNode;
            FingerTable = new FingerTable(localNode, this);
            Successors = successors;
        }

        /// <summary>
        /// Called by the predecessor to a remote node, this acts as a dual heartbeat mechanism and more importantly
        /// notification mechanism between predecessor and successor.
        /// </summary>
        /// <param name="node">A ChordNode instance indicating who the calling node (predecessor) is.</param>
        public void Notify(Node node)
        {
            if (Predecessor == null)
            {
                Predecessor = node;
                return;
            }

            // otherwise, ensure that the predecessor that is calling in
            // is indeed valid...
            if (NodeId.IsIdInRange(node.Id, Predecessor.Id, Id))
            {
                Predecessor = node;
            }
        }

        public async Task<bool> JoinAsync(Node seed)
        {
            SeedNode = seed;
            Log.Information("Joining Ring @ {Host}:{Port}", seed.Host, seed.Port);
            using var client = new NodeClient(seed);
            if (!await IsInstanceValid(client))
            {
                Log.Error("Invalid Node Seed");
                return false;
            }

            try
            {
                var response = await client.FindSuccessorAsync(Id);

                Successor = response.Successor;

                return true;
            }
            catch (Exception exception)
            {
                Log.Error(exception, "Error setting Successor Node ({Message})", exception.Message);
                return false;
            }
        }

        public void Start()
        {
            _maintenanceTask.Dispose();
            _maintenanceTask = Disposable.Sequence(
                _scheduler.SchedulePeriodic(TimeSpan.FromSeconds(1), () => UpdateFingerTable().Wait()),
                _scheduler.SchedulePeriodic(TimeSpan.FromSeconds(5), () => StabilizePredecessors().Wait()),
                _scheduler.SchedulePeriodic(TimeSpan.FromSeconds(5), () => StabilizeSuccessors().Wait()),
                _scheduler.SchedulePeriodic(TimeSpan.FromSeconds(30), () => ReJoin().Wait())
            );
        }

        /// <summary>
        /// Stop the maintenance tasks (asynchronously) that are currently running.
        /// </summary>
        private void StopMaintenance()
        {
            _maintenanceTask.Dispose();
            _maintenanceTask = Disposable.Empty;
        }

        /// <summary>
        /// Find the node that is the rightful owner of a given id.
        /// </summary>
        /// <param name="nodeId">The id whose successor should be found.</param>
        /// <param name="hops">The number of network hops taken in finding the successor.</param>
        /// <returns>The Node that is the Successor of a given ID value.</returns>
        public async Task<(int hops, Node successor)> FindSuccessorAsync(BigInteger nodeId, int hops = 0)
        {
            if (NodeId.IsIdInRange(nodeId, Id, Successor.Id))
            {
                return (hops, Successor);
            }

            var predNode = await FindClosestPrecedingFinger(nodeId);

            using var client = new NodeClient(predNode);
            var result = await client.FindSuccessorAsync(Id, hops);
            return (result.Hops, result.Successor);
        }

        /// <summary>
        /// Returns the closest successor preceding id.
        /// </summary>
        /// <param name="id">The id for which the closest finger should be found</param>
        /// <returns>The successor node of the closest finger to id in the current node's finger table</returns>
        private async Task<Node> FindClosestPrecedingFinger(BigInteger id)
        {
            // iterate downward through the finger table looking for the right finger in the right range. if the finger is
            // in the range but not valid, keep moving. if the entire finger table is checked without success, check the successor
            // cache - if that fails, return the local node as the closest preceding finger.
            return await FindValidNode(FingerTable.Select(x => x.Successor)) ??
                   // at this point, not even the successor is any good so go through the successor cache and run the same test
                   await FindValidNode(Successors) ??
                   // otherwise, if there is nothing closer, the local node is the closest preceding finger
                   LocalNode;

            async Task<Node?> FindValidNode(IEnumerable<Node> nodes)
            {
                foreach (var node in nodes)
                {
                    // if the finger is more closely between the local node and id and that finger corresponds to a valid node, return the finger
                    if (node == LocalNode || !NodeId.FingerInRange(node.Id, Id, id))
                        continue;

                    using var client = new NodeClient(node);
                    if (await IsInstanceValid(client))
                    {
                        return node;
                    }
                }

                return null;
            }
        }

        private static async Task<bool> IsInstanceValid(NodeClient node)
        {
            try
            {
                await node.GetSuccessorAsync();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public void Dispose()
        {
            StopMaintenance();
        }

        /// <summary>
        /// Update the local node's finger table entries on a background thread.
        /// </summary>
        private async Task UpdateFingerTable()
        {
            await FingerTable.UpdateNextAsync();
        }

        /// <summary>
        /// Maintenance task to stabilize the local node's predecessor as per the Chord paper.
        /// </summary>
        /// <param name="sender">The backgroundworker thread that this task is running on.</param>
        /// <param name="ea">Args (ignored)</param>
        private async Task StabilizePredecessors()
        {
            if (Predecessor == null)
                return;

            try
            {
                // validate predecessor (in case of error, predecessor becomes null
                // and is fixed by stabilize successors and notify.
                using var instance = new NodeClient(Predecessor);
                if (!await IsInstanceValid(instance))
                {
                    Predecessor = null;
                }
            }
            catch (Exception e)
            {
                Log.Error("StabilizePredecessors error: {Message}", e.Message);
                Predecessor = null;
            }
        }


        /// <summary>
        /// Maintenance task to ensure that the local node has valid successor node.  Roughly equivalent
        /// to what is called out in the Chord paper.
        /// </summary>
        private async Task StabilizeSuccessors()
        {
            try
            {
                // check in successor and if it's bad, replace it with
                // the next live entry in the successor cache
                using var successorClient = new NodeClient(Successor);
                var succPredNode = await successorClient.GetPredecessorAsync();
                if (succPredNode != null)
                {
                    if (NodeId.IsIdInRange(succPredNode.Id, Id, Successor.Id))
                    {
                        Successor = succPredNode;
                    }

                    // ignoring return because bad node will be detected on next invocation
                    await successorClient.NotifyAsync(LocalNode);
                    await UpdateFromSuccessorCache(Successor);
                }
                else
                {
                    var successorCacheHelped = false;
                    foreach (var entry in Successors)
                    {
                        using var instance = new NodeClient(entry);
                        if (await IsInstanceValid(instance))
                        {
                            Successor = entry;
                            await instance.NotifyAsync(LocalNode);
                            await UpdateFromSuccessorCache(Successor);
                            successorCacheHelped = true;
                            break;
                        }
                    }

                    // if we get here, then we got no help and have no other recourse than to re-join using the initial seed...
                    if (!successorCacheHelped)
                    {
                        Log.Error("Ring consistency error, Re-Joining Chord ring");
                        await JoinAsync(SeedNode);
                        return;
                    }
                }
            }
            catch (Exception exception)
            {
                Log.Error(exception, "Error occured during StabilizeSuccessors ({Message})", exception.Message);
            }
        }


         // toggled on join/first-run of ReJoin to provide some buffer between join and consistency check
        private bool _hasReJoinRun;

        /// <summary>
        /// Maintenance task to perform ring consistency checking and re-joining to keep a Chord
        /// ring stable under extreme churn and in cases of ring damage.
        /// </summary>
        /// <param name="sender">The calling backgroundworker.</param>
        /// <param name="ea">Args (ignored for this task).</param>
        private async Task ReJoin()
        {
            try
            {
                // if this is the first iteration, then the core logic
                // is skipped, as the first iteration generally occurs
                // right after node Join - allowing a short buffer for
                // routing structures to stabilize improves the utility
                // of the ReJoin facility.
                if (_hasReJoinRun)
                {
                    // first find the successor for the seed node
                    var (_, seedSuccessor) = await FindSuccessorAsync(SeedNode.Id);

                    // if the successor is not equal to the seed node, something is fishy
                    if (seedSuccessor.Id == SeedNode.Id)
                        return;

                    // if the seed node is still active, re-join the ring to the seed node
                    using var instance = new NodeClient(SeedNode);
                    if (!await IsInstanceValid(instance))
                        return;

                    Log.Error("Unable to contact initial seed node {SeedNode}.  Re-Joining...", SeedNode);
                    await JoinAsync(SeedNode);

                    // otherwise, in the future, there will be a cache of seed nodes to check/join from...
                    // as it may be the case that the seed node simply has disconnected from the network.
                }
                else
                {
                    // subsequent iterations will go through the core logic
                    _hasReJoinRun = true;
                }
            }
            catch (Exception exception)
            {
                Log.Error(exception, "Error occured during ReJoin ({Message})", exception.Message);
            }
        }


        /// <summary>
        /// Get the successor cache from a remote node and assign an altered version the local successorCache.
        /// Gets the remote successor cache, prepends remoteNode and lops off the last entry from the remote
        /// successorcache.
        /// </summary>
        /// <param name="successor">The remote node to get the succesorCache from.</param>
        private async Task UpdateFromSuccessorCache(Node successor)
        {
            Log.Debug("Updating Successors from successor: {Successor}", successor);
            using var client = new NodeClient(successor);
            var remoteSuccessorCache = await client.GetSuccessorsAsync();

            Successors[0] = successor;
            for (var i = 1; i < Successors.Length; i++)
            {
                Successors[i] = remoteSuccessorCache[i - 1];
            }
        }

        public async Task DepartAsync()
        {
            await Task.CompletedTask;throw new NotImplementedException();
        }
    }
}