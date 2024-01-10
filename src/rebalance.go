package redis_cli

import (
	"math"
	"sort"
	"strconv"
	"strings"
)

func clusterManagerCommandRebalance(argc *Usage) {
	var port int
	var ip string
	var weightedNodes []*ClusterManagerNode
	var involved []*ClusterManagerNode
	var err error
	var node *ClusterManagerNode
	getClusterHostFromCmdArgs(argc.ReBalance, &ip, &port)
	node = clusterManagerNewNode(ip, port)
	err = clusterManagerLoadInfoFromNode(node, 0)
	if err != nil {
		return
	}
	var result, i int
	result = 1
	if config.weight != "" {
		for _, p := range strings.Split(config.weight, ",") {
			var name string
			var w float64
			var n *ClusterManagerNode
			if len(strings.Split(p, "=")) != 2 {
				clusterManagerLogErr("--cluster-weight format error : <node1=w1, ...nodeN=wN>")
			}
			name = strings.Split(p, "=")[0]
			w, err = strconv.ParseFloat(strings.Split(p, "=")[1], 64)
			if err != nil {
				clusterManagerLogErr("--cluster-weight format error : <node1=w1, ...nodeN=wN>")
			}
			n = clusterManagerNodeByAbbreviatedName(name)
			if n == nil {
				clusterManagerLogErr("*** No such master node %s", name)
				return
			}
			n.Weight = w
		}
	}
	var totalWeight float64
	var nodesInvolved int
	var useEmpty int
	useEmpty = config.flags & ClusterManagerCmdFlagEmptymaster
	involved = make([]*ClusterManagerNode, 0)
	for _, n := range clusterManager.Nodes {
		if intToBool(n.Flags&ClusterManagerFlagSlave) || n.Replicate != "" {
			continue
		}
		if !intToBool(useEmpty) && n.SlotsCount == 0 {
			n.Weight = 0
			continue
		}
		totalWeight += n.Weight
		nodesInvolved++
		involved = append(involved, n)
	}
	weightedNodes = make([]*ClusterManagerNode, nodesInvolved)
	if weightedNodes == nil {
		return
	}
	/* Check cluster, only proceed if it looks sane. */
	err = clusterManagerCheckCluster(1)
	if err != nil {
		clusterManagerLogErr("*** Please fix your cluster problems before rebalancing")
		result = 0
		return
	}
	/* Calculate the slots balance for each node. It's the number of
	 * slots the node should lose (if positive) or gain (if negative)
	 * in order to be balanced. */
	var thresholdReached, totalBalance int
	var threshold float64
	threshold = config.threshold
	i = 0
	for _, n := range involved {
		weightedNodes[i] = n
		i++
		var expected int
		expected = int(float64(ClusterManagerSlots) / totalWeight * n.Weight)
		n.Balance = n.SlotsCount - expected
		totalBalance += n.Balance
		/* Compute the percentage of difference between the
		 * expected number of slots and the real one, to see
		 * if it's over the threshold specified by the user. */
		var overThreshold int
		if threshold > 0 {
			if n.SlotsCount > 0 {
				var errPerc float64
				errPerc = math.Abs(100 - (100.0 * float64(expected) / float64(n.SlotsCount)))
				if errPerc > threshold {
					overThreshold = 1
				}
			} else if expected > 1 {
				overThreshold = 1
			}
		}
		if intToBool(overThreshold) {
			thresholdReached = 1
		}
	}
	if !intToBool(thresholdReached) {
		clusterManagerLogWarn("*** No rebalancing needed! All nodes are within the %.2f%% threshold.", config.threshold)
		return
	}
	/* Because of rounding, it is possible that the balance of all nodes
	 * summed does not give 0. Make sure that nodes that have to provide
	 * slots are always matched by nodes receiving slots. */
	for totalBalance > 0 {
		for _, n := range involved {
			if n.Balance <= 0 && totalBalance > 0 {
				n.Balance--
				totalBalance--
			}
		}
	}
	/* Sort nodes by their slots balance. */
	sort.Slice(weightedNodes, func(i, j int) bool {
		return weightedNodes[i].Balance < weightedNodes[j].Balance
	})
	clusterManagerLogInfo(">>> Rebalancing across %d nodes. Total weight = %.2f", nodesInvolved, totalWeight)
	if config.verbose {
		for i = 0; i < nodesInvolved; i++ {
			var n *ClusterManagerNode
			n = weightedNodes[i]
			clusterManagerLogInfo("%s:%d balance is %d slots", n.Ip, n.Port, n.Balance)
		}
	}
	/* Now we have at the start of the 'sn' array nodes that should get
	 * slots, at the end nodes that must give slots.
	 * We take two indexes, one at the start, and one at the end,
	 * incrementing or decrementing the indexes accordingly til we
	 * find nodes that need to get/provide slots. */
	var dstIdx int
	var srcIdx int
	srcIdx = nodesInvolved - 1
	var simulate int
	simulate = config.flags & ClusterManagerCmdFlagSimulate
	for dstIdx < srcIdx {
		var dst *ClusterManagerNode
		var src *ClusterManagerNode
		dst = weightedNodes[dstIdx]
		src = weightedNodes[srcIdx]
		var db int
		var sb int
		db = int(math.Abs(float64(dst.Balance)))
		sb = int(math.Abs(float64(src.Balance)))
		var numSlots int
		if db < sb {
			numSlots = db
		} else {
			numSlots = sb
		}
		if numSlots > 0 {
			clusterManagerLogInfo("Moving %d slots from %s:%d to %s:%d", numSlots, src.Ip, src.Port, dst.Ip, dst.Port)
			/* Actually move the slots. */
			var lsrc []*ClusterManagerNode
			var table []*clusterManagerReshardTableItem
			lsrc = make([]*ClusterManagerNode, 0)
			lsrc = append(lsrc, src)
			table = clusterManagerComputeReshardTable(lsrc, numSlots)
			var tableLen int
			tableLen = len(table)
			if table == nil || tableLen != numSlots {
				clusterManagerLogErr("*** Assertion failed: Reshard table != number of slots")
				result = 0
				return
			}
			if intToBool(simulate) {
				for i = 0; i < tableLen; i++ {
					clusterManagerLogInfof("#")
				}
			} else {
				var opts int
				opts = ClusterManagerOptQuiet | ClusterManagerOptUpdate
				for _, item := range table {
					result = clusterManagerMoveSlot(item.source, dst, item.slot, opts, nil)
					if !intToBool(result) {
						return
					}
					//clusterManagerLogInfof("#")
				}
			}
			clusterManagerLogInfof("\n")
		}
		/* Update nodes balance. */
		dst.Balance += numSlots
		src.Balance -= numSlots
		if dst.Balance == 0 {
			dstIdx++
		}
		if src.Balance == 0 {
			srcIdx--
		}
	}
}

/* Like clusterManagerNodeByName but the specified name can be just the first
 * part of the node ID as long as the prefix in unique across the
 * cluster.
 */
func clusterManagerNodeByAbbreviatedName(name string) (found *ClusterManagerNode) {
	if clusterManager.Nodes == nil {
		return nil
	}
	var lcname string
	lcname = name
	lcname = strings.ToLower(lcname)
	for _, n := range clusterManager.Nodes {
		if n.Name != "" && lcname == n.Name {
			found = n
			break
		}
	}
	return nil
}
