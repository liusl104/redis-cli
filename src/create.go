package redis_cli

import (
	"fmt"
	"github.com/siddontang/go-log/log"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

/* Data structure used to represent a sequence of cluster Nodes. */
type clusterManagerNodeArray struct {
	Nodes      []*ClusterManagerNode
	Allocation []*ClusterManagerNode
	Len        int
	Count      int
}

func clusterManagerPrintReplyError(n *ClusterManagerNode, err error) {
	clusterManagerLogErr("Node %s:%d replied with error:\n%s", n.Ip, n.Port, err.Error())
}

func clusterManagerPrintAuthError(n *ClusterManagerNode, err error) {
	clusterManagerLogErr("[ERR] Node %s:%d %s", n.Ip, n.Port, err.Error())
}

func clusterManagerPrintNotEmptyNodeError(node *ClusterManagerNode, err error) {
	var msg string
	if err != nil {
		msg = err.Error()
	} else {
		msg = "is not empty. Either the node already knows other Nodes (check with CLUSTER NODES) or contains some key in database 0."
	}
	clusterManagerLogErr("[ERR] Node %s:%d %s", node.Ip, node.Port, msg)
}

/* clusterManagerCommandCreate Cluster Manager Commands */
func clusterManagerCommandCreate(argc *Usage) {
	var i, j int
	var err error
	// var success int
	// success = 1
	if clusterManager.Nodes == nil {
		clusterManager.Nodes = make([]*ClusterManagerNode, 0)
	}
	for _, addr := range strings.Split(argc.Create, ",") {
		if !strings.Contains(addr, ":") {
			clusterManagerLogErr("Invalid address format: %s", addr)
			return
		}
		var ip string
		var port int
		getClusterHostFromCmdArgs(addr, &ip, &port)
		var node *ClusterManagerNode
		node = clusterManagerNewNode(ip, port)
		err = clusterManagerNodeConnect(node)
		if err != nil {
			clusterManagerPrintReplyError(node, err)
			return
		}
		_, err = clusterManagerNodeIsCluster(node)
		if err != nil {
			clusterManagerPrintNotClusterNodeError(node, err)
		}
		err = clusterManagerNodeLoadInfo(node, 0)
		if err != nil {
			clusterManagerPrintReplyError(node, err)
			return
		}
		if !clusterManagerNodeIsEmpty(node, &err) {
			clusterManagerPrintNotEmptyNodeError(node, err)
			return
		}
		clusterManager.Nodes = append(clusterManager.Nodes, node)
	}
	nodeLen := len(clusterManager.Nodes)
	replicas := config.replicas
	mastersCount := clusterManagerMastersCount(nodeLen, replicas)
	if mastersCount < 3 {
		clusterManagerLogErr(
			"*** ERROR: Invalid configuration for cluster creation.\n"+
				"*** Redis Cluster requires at least 3 master nodes.\n"+
				"*** This is not possible with %d nodes and %d replicas per node.",
			nodeLen, replicas)
		clusterManagerLogErr("*** At least %d nodes are required.", 3*(replicas+1))
		return
	}
	clusterManagerLogInfo(">>> Performing hash slots allocation on %d nodes...", nodeLen)
	var interleavedLen, ipCount int
	var interleaved = make([]*ClusterManagerNode, nodeLen)
	var ips = make([]string, nodeLen)
	var ipNodes = make([]*clusterManagerNodeArray, nodeLen)

	// Init ipNodes
	for x := 0; x < nodeLen; x++ {
		ipNodes[x] = new(clusterManagerNodeArray)
	}

	for _, n := range clusterManager.Nodes {
		var found int
		for i = 0; i < ipCount; i++ {
			ip := ips[i]
			if !intToBool(strings.Compare(ip, n.Ip)) {
				found = 1
				break
			}
		}
		if !intToBool(found) {
			ips = append(ips, n.Ip)
			ipCount++
		}
		nodeArray := ipNodes[i]
		if nodeArray.Nodes == nil {
			clusterManagerNodeArrayInit(nodeArray, nodeLen)
		}
		clusterManagerNodeArrayAdd(nodeArray, n)
		ipNodes[i] = nodeArray
	}
	for interleavedLen < nodeLen {
		for i = 0; i < ipCount; i++ {
			var node_array *clusterManagerNodeArray = ipNodes[i]
			if node_array.Count > 0 {
				var n *ClusterManagerNode
				n = clusterManagerNodeArrayShift(node_array)
				interleaved[interleavedLen] = n
				interleavedLen++
			}
		}
	}
	var masters []*ClusterManagerNode
	masters = interleaved
	interleaved = interleaved[len(interleaved)%mastersCount:]
	interleavedLen -= mastersCount
	slotsPerNode := float64(ClusterManagerSlots) / float64(mastersCount)
	var first int
	var cursor float64
	for i = 0; i < mastersCount; i++ {
		var master *ClusterManagerNode = masters[i]
		// lround 四舍五入
		var last int
		last = int(math.Round(cursor + slotsPerNode - 1))
		if last > ClusterManagerSlots || i == mastersCount-1 {
			last = ClusterManagerSlots - 1
		}
		if last < first {
			last = first
		}
		clusterManagerLogInfo("Master[%d] -> Slots %d - %d", i, first, last)
		master.SlotsCount = 0
		master.Slots = []int{}
		for j = first; j <= last; j++ {
			master.Slots = append(master.Slots, j)
			master.SlotsCount++
		}
		master.Dirty = 1
		first = last + 1
		cursor += slotsPerNode
	}
	/* Rotating the list sometimes helps to get better initial
	 * anti-affinity before the optimizer runs. */
	var firstNode *ClusterManagerNode = interleaved[0]

	for i = 0; i < (interleavedLen - 1); i++ {
		interleaved[i] = interleaved[i+1]
	}
	if interleavedLen-1 < 0 {
		interleaved[0] = firstNode
	} else {
		interleaved[interleavedLen-1] = firstNode
	}
	var assignUnused int
	var availableCount = interleavedLen
assignReplicas:
	for i = 0; i < mastersCount; i++ {
		var master *ClusterManagerNode = masters[i]
		var assignedReplicas int = 0
		for assignedReplicas < replicas {
			if availableCount == 0 {
				break
			}
			var found, slave *ClusterManagerNode
			var firstNodeIdx int = -1
			for j = 0; j < interleavedLen; j++ {
				var n *ClusterManagerNode = interleaved[j]
				if n == nil {
					continue
				}
				if intToBool(strings.Compare(n.Ip, master.Ip)) {
					found = n
					interleaved[j] = nil
					break
				}
				if firstNodeIdx < 0 {
					firstNodeIdx = j
				}
			}
			if found != nil {
				slave = found
			} else if firstNodeIdx >= 0 {
				slave = interleaved[firstNodeIdx]
				interleavedLen -= len(interleaved) - (len(interleaved) + firstNodeIdx)
				interleaved = interleaved[:firstNodeIdx+1]
				// interleaved += firstNodeIdx+1
			}
			if slave != nil {
				assignedReplicas++
				availableCount--
				if slave.Replicate != "" {
					slave.Replicate = ""
				}
				slave.Replicate = master.Name
				slave.Dirty = 1
			} else {
				break
			}
			clusterManagerLogInfo("Adding replica %s:%d to %s:%d", slave.Ip, slave.Port, master.Ip, master.Port)
			if intToBool(assignUnused) {
				break
			}
		}
	}
	if !intToBool(assignUnused) && availableCount > 0 {
		assignUnused = 1
		clusterManagerLogInfo("Adding extra replicas...")
		goto assignReplicas
	}
	for i = 0; i < ipCount; i++ {
		var nodeArray *clusterManagerNodeArray
		nodeArray = ipNodes[i]
		clusterManagerNodeArrayReset(nodeArray)
	}
	clusterManagerOptimizeAntiAffinity(ipNodes, ipCount)
	clusterManagerShowNodes()
	var ignoreForce int
	if intToBool(confirmWithYes("Can I set the above configuration?", ignoreForce)) {
		for _, node := range clusterManager.Nodes {
			var flushed int
			flushed, err = clusterManagerFlushNodeConfig(node)
			if !intToBool(flushed) && intToBool(node.Dirty) && node.Replicate == "" {
				if err != nil {
					clusterManagerPrintReplyError(node, err)
				}
				// success = 0
				return
			} else {
				if err != nil {
					err = nil
				}
			}
		}
		clusterManagerLogInfo(">>> Nodes configuration updated")
		clusterManagerLogInfo(">>> Assign a different config epoch to each node")
		var configEpoch int
		configEpoch = 1
		for _, node := range clusterManager.Nodes {
			_, err = node.Context.Conn.Do("cluster", "set-config-epoch", configEpoch)
			configEpoch++
			if err != nil {
				clusterManagerPrintReplyError(node, err)
				// success = 0
				return
			}
		}
		clusterManagerLogInfo(">>> Sending CLUSTER MEET messages to join the cluster")
		// var firstNode *ClusterManagerNode
		firstNode = nil
		for _, node := range clusterManager.Nodes {
			if firstNode == nil {
				firstNode = node
				continue
			}
			_, err = node.Context.Conn.Do("cluster", "meet", firstNode.Ip, firstNode.Port)
			if err != nil {
				msg := "Failed to send CLUSTER MEET command"
				clusterManagerPrintReplyError(node, fmt.Errorf(msg))
				return
			}
		}
		/* Give one second for the join to start, in order to avoid that
		 * waiting for cluster join will find all the nodes agree about
		 * the config as they are still empty with unassigned slots. */
		time.Sleep(time.Duration(1) * time.Second)
		clusterManagerWaitForClusterJoin()
		/* Useful for the replicas */
		for _, node := range clusterManager.Nodes {
			if !intToBool(node.Dirty) {
				continue
			}
			_, err = clusterManagerFlushNodeConfig(node)
			if err != nil && node.Replicate == "" {
				clusterManagerPrintReplyError(node, err)
				return
			}
		}
		// Reset Nodes
		firstNode = nil
		for _, node := range clusterManager.Nodes {
			if firstNode == nil {
				firstNode = node
				break
			}
		}
		clusterManager.Nodes = nil
		err = clusterManagerLoadInfoFromNode(firstNode, 0)
		if err != nil {
			clusterManagerPrintReplyError(firstNode, err)
			return
		}
		err = clusterManagerCheckCluster(0)
		if err != nil {
			clusterManagerPrintReplyError(firstNode, err)
			return
		}
	}
}

/*clusterManagerNodeArrayReset Reset array->nodes to the original array allocation and re-count non-NULL  nodes. */
func clusterManagerNodeArrayReset(array *clusterManagerNodeArray) *clusterManagerNodeArray {
	if len(array.Nodes) > len(array.Allocation) {
		array.Len = len(array.Nodes) - len(array.Allocation)
		array.Nodes = array.Allocation
		array.Count = 0
		for i := 0; i < array.Len; i++ {
			if array.Nodes[i] != nil {
				array.Count++
			}
		}
	}
	return array
}

/* clusterManagerWaitForClusterJoin Wait until the cluster configuration is consistent. */
func clusterManagerWaitForClusterJoin() {
	clusterManagerLogInfof("Waiting for the cluster to join")
	var counter int
	var checkAfter int
	checkAfter = ClusterJoinCheckAfter + int(float64(len(clusterManager.Nodes))*0.15)
	for !clusterManagerIsConfigConsistent() {
		fmt.Printf(".")
		time.Sleep(time.Second * time.Duration(1))
		counter++
		if counter > checkAfter {
			status := clusterManagerGetLinkStatus()
			if status != nil && len(status) > 0 {
				clusterManagerLogErr(fmt.Sprintf("Warning: %d node(s) may  be unreachable", len(status)))
				for key, value := range status {
					nodeAddr := key
					var nodeIP string
					var nodePort, nodeBusPort int
					from := [...]string{value}
					if parseClusterNodeAddress(&nodeAddr, &nodeIP, &nodePort, &nodeBusPort) && intToBool(nodeBusPort) {
						clusterManagerLogErr(fmt.Sprintf(" - The port %d of node %s may be unreachable from:", nodeBusPort, nodeIP))
					} else {
						clusterManagerLogErr(fmt.Sprintf(" - Node %s may be unreachable from:", nodeAddr))
					}
					for _, fromAddr := range from {
						clusterManagerLogErr(fmt.Sprintf("   %s", fromAddr))
					}
					clusterManagerLogErr("Cluster bus ports must be reachable " +
						"by every node.\nRemember that " +
						"cluster bus ports are different " +
						"from standard instance ports.\n")
				}
			}
			if status != nil {
				status = nil
			}
			counter = 0
		}
	}
	defer fmt.Printf("\n")
}

func parseClusterNodeAddress(addr *string, ipPtr *string, portPtr *int, busPortPtr *int) bool {
	c := strings.Split(*addr, "@")
	if len(c) >= 2 {
		if busPortPtr != nil {
			*busPortPtr, _ = strconv.Atoi(c[1])

		}
	}
	c = strings.Split(*addr, ":")
	if len(c) >= 2 {
		*ipPtr = *addr
		*portPtr, _ = strconv.Atoi(c[1])
	} else {
		return false
	}

	return true
}

/* clusterManagerGetLinkStatus Check for disconnected cluster links. It returns a dict whose keys
 * are the unreachable node addresses and the values are lists of
 * node addresses that cannot reach the unreachable node. */
func clusterManagerGetLinkStatus() map[string]string {
	if clusterManager.Nodes == nil {
		return nil
	}
	var status map[string]string
	status = make(map[string]string, 0)
	for _, node := range clusterManager.Nodes {
		links := clusterManagerGetDisconnectedLinks(node)
		if links != nil {
			for _, link := range links {
				var from []string
				if _, ok := status[link.nodeAddr]; ok {
					if from == nil {
						from = make([]string, 0)
						from = append(from, status[link.nodeAddr])
					} else {
						from = append(from, status[link.nodeAddr])
					}
				}
				var myaddr string
				myaddr = fmt.Sprintf("%s:%d", node.Ip, node.Port)
				from = append(from, myaddr)

			}
		}
	}
	return status
}

type clusterManagerLink struct {
	nodeName    string
	nodeAddr    string
	connected   int
	handshaking int
}

func clusterManagerGetDisconnectedLinks(node *ClusterManagerNode) (links []*clusterManagerLink) {
	var reply string
	var err error
	reply, err = node.Context.Do("CLUSTER", "NODES")
	if err != nil {
		return
	}
	links = make([]*clusterManagerLink, 0)
	lines := strings.Split(strings.Trim(reply, "\n"), "\n")
	for _, line := range lines {
		var nodeName, addr, flags, linkStatus string
		token := strings.Split(line, " ")
		nodeName = token[0]
		addr = token[1]
		flags = token[2]
		linkStatus = token[7]
		if nodeName == "" || addr == "" || flags == "" || linkStatus == "" {
			continue
		}
		if strings.Contains(flags, "myself") {
			continue
		}
		var disconnected, handshaking int
		if strings.Contains(flags, "disconnected") || strings.Contains(linkStatus, "disconnected") {
			disconnected = 1
		}
		if strings.Contains(flags, "handshake") {
			handshaking = 1
		}
		if intToBool(disconnected) || intToBool(handshaking) {
			link := new(clusterManagerLink)
			link.nodeName = nodeName
			link.nodeAddr = addr
			link.connected = 0
			link.handshaking = handshaking
			links = append(links, link)
		}
	}
	return
}

/* clusterManagerFlushNodeConfig Flush the dirty node configuration by calling replicate for slaves or
 * adding the slots defined in the masters. */
func clusterManagerFlushNodeConfig(node *ClusterManagerNode) (success int, err error) {
	if !intToBool(node.Dirty) {
		return 0, nil
	}
	var reply string
	// var isErr int
	success = 1
	if node.Replicate != "" {
		reply, err = node.Context.Do("CLUSTER", "REPLICATE", node.Replicate)
		if reply == "" || err != nil {
			success = 0
			/* If the cluster did not already joined it is possible that
			 * the slave does not know the master node yet. So on errors
			 * we return ASAP leaving the dirty flag set, to flush the
			 * config later. */
			return success, err
		}
	} else {
		var added int
		added, err = clusterManagerAddSlots(node)
		if !intToBool(added) || err != nil {
			success = 0
		}
	}
	node.Dirty = 0
	return
}

/* clusterManagerAddSlots Flush dirty slots configuration of the node by calling CLUSTER ADDSLOTS */
func clusterManagerAddSlots(node *ClusterManagerNode) (success int, err error) {
	// var reply string
	success = 1
	/* First two args are used for the command itself. */
	var argv []interface{}
	argv = append(argv, "ADDSLOTS")
	for _, i := range node.Slots {
		if i >= 0 && i < ClusterManagerSlots {
			argv = append(argv, i)
		} else {
			return 0, fmt.Errorf("slot not in %d", ClusterManagerSlots)
		}
	}
	_, err = node.Context.Conn.Do("CLUSTER", argv...)
	if err != nil {
		success = 0
	}
	return success, err
}
func confirmWithYes(msg string, ignoreForce int) int {
	/* if --cluster-yes option is set and ignore_force is false,
	 * do not prompt for an answer */
	if !intToBool(ignoreForce) && intToBool(config.flags&ClusterManagerCmdFlagYes) {
		return 1
	}
	// clusterManagerLogInfo(msg)
	var confirm string
	fmt.Printf("%s (type 'yes' to accept): ", msg)
	n, err := fmt.Scanln(&confirm)
	if err != nil {
		return 0
	}
	if n == 0 {
		return 0
	}
	if strings.ToLower(strings.Trim(confirm, " ")) == "yes" {
		return 1
	}
	return 0
}
func clusterManagerOptimizeAntiAffinity(ipNode []*clusterManagerNodeArray, ipCount int) {
	var offenders []*ClusterManagerNode
	score := clusterManagerGetAntiAffinityScore(ipNode, ipCount, nil, nil)
	if score == 0 {
		return
	}
	clusterManagerLogInfo(">>> Trying to optimize slaves allocation for anti-affinity")
	nodeLen := len(clusterManager.Nodes)
	maxIter := 500 * nodeLen // Effort is proportional to cluster size...
	// srand(time(NULL));
	// rand.Seed(time.Now().Unix())
	for maxIter > 0 {
		var offendingLen int
		if offenders != nil {
			offenders = nil
		}
		score = clusterManagerGetAntiAffinityScore(ipNode, ipCount, &offenders, &offendingLen)
		if score == 0 || offendingLen == 0 {
			// Optimal anti affinity reached
			break
		}
		/* We'll try to randomly swap a slave's assigned master causing
		 * an affinity problem with another random slave, to see if we
		 * can improve the affinity. */

		var randIdx int

		randIdx = rand.Int() % offendingLen
		var first *ClusterManagerNode
		first = offenders[randIdx]
		var second *ClusterManagerNode
		otherReplicas := make([]*ClusterManagerNode, nodeLen-1)
		var otherReplicasCount int
		for _, n := range clusterManager.Nodes {
			if n != first && n.Replicate != "" {
				otherReplicas[otherReplicasCount] = n
				otherReplicasCount++
			}
		}
		if otherReplicasCount == 0 {
			break
		}
		randIdx = rand.Int() % otherReplicasCount
		second = otherReplicas[randIdx]
		firstMaster := &first.Replicate
		secondMaster := &second.Replicate
		first.Replicate = *secondMaster
		first.Dirty = 1
		second.Replicate = *firstMaster
		second.Dirty = 1
		newScore := clusterManagerGetAntiAffinityScore(ipNode, ipCount, nil, nil)
		/* If the change actually makes thing worse, revert. Otherwise
		 * leave as it is because the best solution may need a few
		 * combined swaps. */
		if newScore > score {
			first.Replicate = *firstMaster
			second.Replicate = *secondMaster
		}
		maxIter--
	}
	score = clusterManagerGetAntiAffinityScore(ipNode, ipCount, nil, nil)
	var msg string
	var perfect bool
	if score == 0 {
		perfect = true
	} else {
		perfect = false
	}
	if perfect {
		msg = "[OK] Perfect anti-affinity obtained!"
		clusterManagerLogOk(msg)
	} else if score >= 10000 {
		msg = "[WARNING] Some slaves are in the same host as their master"
		clusterManagerLogWarn(msg)
	} else {
		msg = "[WARNING] Some slaves of the same master are in the same host"
		clusterManagerLogOk(msg)
	}

}

/*
	clusterManagerGetAntiAffinityScore Return the anti-affinity score, which is a measure of the amount of

* violations of anti-affinity in the current cluster layout, that is, how
* badly the masters and slaves are distributed in the different IP
* addresses so that slaves of the same master are not in the master
* host and are also in different hosts.
*
* The score is calculated as follows:
*
  - SAME_AS_MASTER = 10000 * each slave in the same IP of its master.
  - SAME_AS_SLAVE  = 1 * each slave having the same IP as another slave
    of the same master.
  - FINAL_SCORE = SAME_AS_MASTER + SAME_AS_SLAVE

*
* So a greater score means a worse anti-affinity level, while zero
* means perfect anti-affinity.
*
* The anti affinity optimizator will try to get a score as low as
* possible. Since we do not want to sacrifice the fact that slaves should
* not be in the same host as the master, we assign 10000 times the score
* to this violation, so that we'll optimize for the second factor only
* if it does not impact the first one.
*
* The ipnodes argument is an array of clusterManagerNodeArray, one for
* each IP, while ip_count is the total number of IPs in the configuration.
*
* The function returns the above score, and the list of
* offending slaves can be stored into the 'offending' argument,
* so that the optimizer can try changing the configuration of the
* slaves violating the anti-affinity goals.
*/
func clusterManagerGetAntiAffinityScore(ipNodes []*clusterManagerNodeArray, ipCount int, offending *[]*ClusterManagerNode, offendingLen *int) int {
	var score, i, j, nodeLen int
	nodeLen = len(clusterManager.Nodes)
	var offendingP []*ClusterManagerNode
	if offending != nil {
		*offending = make([]*ClusterManagerNode, nodeLen)
		offendingP = *offending
	}
	/* For each set of nodes in the same host, split by
	 * related nodes (masters and slaves which are involved in
	 * replication of each other) */
	for i = 0; i < ipCount; i++ {
		var nodeArray *clusterManagerNodeArray
		nodeArray = ipNodes[i]
		related := make(map[string]string, 0)
		var ip string
		for j = 0; j < nodeArray.Len; j++ {
			var node *ClusterManagerNode
			node = nodeArray.Nodes[j]
			if node == nil {
				continue
			}
			if ip == "" {
				ip = node.Ip
			}
			/* We always use the Master ID as key. */
			var key string
			if node.Replicate != "" {
				key = node.Name
			} else {
				key = node.Replicate
			}
			var types string
			if key == "" {
				log.Fatalf("key is null")
			}
			if _, ok := related[key]; ok {
				types = related[key]
			}
			/* Master type 'm' is always set as the first character of the
			 * types string. */
			if node.Replicate != "" {
				types += "s"
			} else {
				s := fmt.Sprintf("m%s", types)
				types = s
			}
			related[key] = types
		}
		/* Now it's trivial to check, for each related group having the
		 * same host, what is their local score. */
		for name, types := range related {
			typesLen := len(types)
			if typesLen < 2 {
				continue
			}
			if types[0] == 'm' {
				score += 10000 * (typesLen - 1)
			} else {
				score += 1 * typesLen
			}
			if offending == nil {
				continue
			}
			/* Populate the list of offending nodes. */
			for _, n := range clusterManager.Nodes {
				if n.Replicate == "" {
					continue
				}
				if !intToBool(strings.Compare(n.Replicate, name)) && !intToBool(strings.Compare(n.Ip, ip)) {
					offendingP = append(*offending, n)
					if *offendingLen != 0 {
						*offendingLen++
					}
					break
				}
			}
		}
	}
	log.Debugf("offendingP, %d", len(offendingP))
	return score
}

/* clusterManagerNodeArrayShift Shift array->nodes and store the shifted node into 'nodeptr'. */
func clusterManagerNodeArrayShift(array *clusterManagerNodeArray) (nodePtr *ClusterManagerNode) {
	if array.Len > 0 {
		/* If the first node to be shifted is not NULL, decrement count. */
		if array.Nodes[0] != nil {
			array.Count--
		}
		/* Store the first node to be shifted into 'nodeptr'. */
		nodePtr = array.Nodes[0]
		/* Shift the nodes array and decrement length. */
		// array.NodesOffset++
		array.Nodes = array.Nodes[1:]
		array.Len--

	} else {
		clusterManagerLogErr("Cannot shift from an empty array")
	}

	return
}

func clusterManagerNodeArrayInit(array *clusterManagerNodeArray, allocLen int) {
	if array == nil {
		array = new(clusterManagerNodeArray)
	}
	array.Nodes = make([]*ClusterManagerNode, allocLen)
	array.Allocation = array.Nodes
	array.Len = allocLen
	array.Count = 0
	return
}

func clusterManagerNodeArrayAdd(array *clusterManagerNodeArray, node *ClusterManagerNode) {
	//count := array.Count
	if array.Len <= 0 {
		log.Fatalf("array len is %d", array.Len)
	}
	if node == nil {
		log.Fatalf("array node is nil")
	}
	if array.Count >= array.Len {
		log.Fatalf("array count > array len")
	}
	array.Nodes[array.Count] = node
	array.Count++

	return
}
func clusterManagerMastersCount(nodes, replicas int) int {
	return nodes / (replicas + 1)
}

/* clusterManagerNodeIsEmpty Checks whether the node is empty. Node is considered not-empty if it has
 * some key or if it already knows other Nodes */
func clusterManagerNodeIsEmpty(node *ClusterManagerNode, err *error) (isEmpty bool) {
	var info string
	info, *err = clusterManagerGetNodeRedisInfo(node)
	if info == "" {
		return false
	}
	// 如果找到了db0:就返回
	if strings.Contains(info, "db0:") {
		isEmpty = false
		return
	}
	info, *err = node.Context.Do("CLUSTER", "INFO")
	if *err != nil {
		isEmpty = false
		return
	}
	var knownNodes int
	knownNodes, *err = getLongInfoField(&info, "cluster_known_nodes")
	if knownNodes == 1 {
		isEmpty = true
	} else {
		isEmpty = false
	}

	return
}
