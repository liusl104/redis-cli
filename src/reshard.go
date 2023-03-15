package redis_cli

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"math"
	"sort"
	"strings"
)

func clusterManagerCommandReshard(argc *Usage) {
	var port int
	var ip string
	var err error
	getClusterHostFromCmdArgs(argc.ReShard, &ip, &port)
	var node *ClusterManagerNode
	node = clusterManagerNewNode(ip, port)
	err = clusterManagerLoadInfoFromNode(node, 0)
	if err != nil {
		clusterManagerPrintAuthError(node, err)
		return
	}
	err = clusterManagerCheckCluster(0)
	if err != nil {
		clusterManagerLogErr("*** Please fix your cluster problems before resharding")
		return
	}
	var slots int
	slots = config.slots
	if !intToBool(slots) {
		for slots <= 0 || slots > ClusterManagerSlots {
			clusterManagerLogInfof("How many slots do you want to move (from 1 to %d)? ", ClusterManagerSlots)
			n, _ := fmt.Scanln(&slots)
			if n <= 0 {
				continue
			}
		}
	}
	var to string
	var from string
	to = config.to
	from = config.from
	for to == "" {
		clusterManagerLogInfof("What is the receiving node ID? ")
		n, _ := fmt.Scanln(&to)
		if n <= 0 {
			continue
		}
	}
	var raiseErr int
	var target *ClusterManagerNode
	target = clusterNodeForResharding(to, nil, &raiseErr)
	if target == nil {
		return
	}
	var all int
	var result int
	var sources []*ClusterManagerNode
	var table []*clusterManagerReshardTableItem
	if sources == nil {
		sources = make([]*ClusterManagerNode, 0)
	}
	result = 1
	if from == "" {
		clusterManagerLogInfo("Please enter all the source node IDs.")
		clusterManagerLogInfo("  Type 'all' to use all the nodes as source nodes for the hash slots.")
		clusterManagerLogInfo("  Type 'done' once you entered all the source nodes IDs.")
		for {
			var buf string
			clusterManagerLogInfof("Source node #%d: ", len(sources)+1)
			nread, _ := fmt.Scanln(&buf)
			if nread <= 0 {
				continue
			}
			if diffIntToBool(strings.Compare(strings.ToLower(buf), "done")) {
				break
			} else if diffIntToBool(strings.Compare(strings.ToLower(buf), "all")) {
				all = 1
				break
			} else {
				var src *ClusterManagerNode
				src = clusterNodeForResharding(buf, target, &raiseErr)
				if src != nil {
					sources = append(sources, src)
				} else if intToBool(raiseErr) {
					result = 0
					return
				}
			}
		}
	} else {
		var p string

		for _, p = range strings.Split(from, ",") {
			if diffIntToBool(strings.Compare(strings.ToLower(p), "all")) {
				all = 1
				break
			} else {
				var src *ClusterManagerNode
				src = clusterNodeForResharding(p, target, &raiseErr)
				if src != nil {
					sources = append(sources, src)
				} else if intToBool(raiseErr) {
					result = 0
					return
				}
			}
		}

		/* Check if there's still another source to process. */
		/*if !intToBool(all) && len(from) > 0 {
			if diffIntToBool(strings.Compare(strings.ToLower(from), "all")) {
				all = 1
			}
			if !intToBool(all) {
				var src *ClusterManagerNode
				src = clusterNodeForResharding(from, target, &raiseErr)
				if src != nil {
					sources = append(sources, src)
				} else if intToBool(raiseErr) {
					result = 0
					return
				}
			}
		}*/
	}
	if intToBool(all) {
		sources = make([]*ClusterManagerNode, 0)
		for _, n := range clusterManager.Nodes {
			if intToBool(n.Flags&ClusterManagerFlagSlave) || n.Replicate != "" {
				continue
			}
			if diffIntToBool(strings.Compare(n.Name, target.Name)) {
				continue
			}
		}
	}
	if len(sources) == 0 {
		clusterManagerLogErr("*** No source nodes given, operation aborted.")
		result = 0
		return
	}
	clusterManagerLogInfo("Ready to move %d slots.", slots)
	clusterManagerLogInfo("  Source nodes:")
	for _, src := range sources {
		var info string
		info = clusterManagerNodeInfo(src, 4)
		clusterManagerLogInfo(info)
	}
	clusterManagerLogInfo("  Destination node:")
	var info string
	info = clusterManagerNodeInfo(target, 4)
	clusterManagerLogInfo(info)
	table = clusterManagerComputeReshardTable(sources, slots)
	clusterManagerLogInfo("  Resharding plan:")
	clusterManagerShowReshardTable(table)
	if !intToBool(config.flags & ClusterManagerCmdFlagYes) {
		clusterManagerLogInfof("Do you want to proceed with the proposed reshard plan (yes/no)? ")
		var buf string
		n, _ := fmt.Scanln(&buf)
		if n <= 0 {
			result = 0
			return
		}
		if !diffIntToBool(strings.Compare(strings.ToLower(buf), "yes")) {
			result = 0
			return
		}
	}
	var opts int
	opts = ClusterManagerOptVerbose
	for _, item := range table {
		err = nil
		result = clusterManagerMoveSlot(item.source, target, item.slot, opts, err)
		if !intToBool(result) {
			if err != nil {
				clusterManagerLogErr(err.Error())
				return
			}
		}
	}
}

/* clusterManagerMoveSlot Move slots between source and target nodes using MIGRATE.
 *
 * Options:
 * ClusterManagerOptVerbose -- Print a dot for every moved key.
 * ClusterManagerOptCold    -- Move keys without opening slots /
 *                                reconfiguring the nodes.
 * ClusterManagerOptUpdate  -- Update node->slots for source/target nodes.
 * ClusterManagerOptQuiet   -- Don't print info messages.
 */
func clusterManagerMoveSlot(source *ClusterManagerNode, target *ClusterManagerNode, slot int, opts int, err error) int {
	if !intToBool(opts & ClusterManagerOptQuiet) {
		clusterManagerLogInfof("Moving slot %d from %s:%d to %s:%d: ", slot, source.Ip, source.Port, target.Ip, target.Port)
	}
	if err != nil {
		err = nil
	}
	var pipeline, timeout, printDots, optionCold, success int
	pipeline = config.pipeline
	timeout = config.timeout
	printDots = opts & ClusterManagerOptVerbose
	optionCold = opts & ClusterManagerOptCold
	success = 1
	if !intToBool(optionCold) {
		success = clusterManagerSetSlot(target, source, slot, "importing", err)
		if !intToBool(success) {
			return 0
		}
		success = clusterManagerSetSlot(source, target, slot, "migrating", err)
		if !intToBool(success) {
			return 0
		}
	}
	clusterManagerMigrateKeysInSlot(source, target, slot, timeout, pipeline, printDots, err)
	if !intToBool(opts & ClusterManagerOptQuiet) {
		clusterManagerLogInfof("\n")
	}
	if !intToBool(success) {
		return 0
	}
	/* Set the new node as the owner of the slot in all the known nodes. */
	if !intToBool(optionCold) {
		for _, n := range clusterManager.Nodes {
			if intToBool(n.Flags & ClusterManagerFlagSlave) {
				continue
			}
			_, err = n.Context.Do("CLUSTER", "SETSLOT", slot, "NODE", target.Name)
			if err != nil {
				clusterManagerPrintReplyError(n, err)
				return 0
			}
		}
	}
	/* Update the node logical config */
	if intToBool(opts & ClusterManagerOptUpdate) {
		source.Slots = deleteSlice(source.Slots, slot)
		source.SlotsCount--
		target.Slots = append(target.Slots, slot)
		target.SlotsCount++
	}
	return 1
}

/* clusterManagerSetSlot Set slot status to "importing" or "migrating" */
func clusterManagerSetSlot(node1 *ClusterManagerNode, node2 *ClusterManagerNode, slot int, status string, err error) int {
	var reply string
	var success int
	reply, err = node1.Context.Do("CLUSTER", "SETSLOT", slot, status, node2.Name)
	if reply == "" {
		return 0
	}
	success = 1
	if err != nil {
		success = 0
		clusterManagerPrintReplyError(node1, err)
	}
	return success
}

/* clusterManagerMigrateKeysInSlot Migrate all keys in the given slot from source to target.*/
func clusterManagerMigrateKeysInSlot(source *ClusterManagerNode, target *ClusterManagerNode, slot int, timeout int, pipeline int, verbose int, err error) int {
	var success, doFix, doReplace int
	doFix = config.flags & ClusterManagerCmdFlagFix
	doReplace = config.flags & ClusterManagerCmdFlagReplace
	success = 1
	for {
		var dots string
		var reply interface{}
		var count int
		var migrateReply string
		var isBusy bool
		var notServed int
		var getOwnerErr error
		var servedBy *ClusterManagerNode
		var diffs []interface{}
		reply, err = source.Context.Conn.Do("CLUSTER", "GETKEYSINSLOT", slot, pipeline)
		if err != nil {
			success = 0
			clusterManagerPrintReplyError(source, err)
			goto next
		}
		count = len(reply.([]interface{}))
		if count == 0 {
			break
		}
		/*if intToBool(verbose) {
				dots = count + 1
		}*/
		/* Calling MIGRATE command. */
		migrateReply, err = clusterManagerMigrateKeysInReply(source, target, reply, 0, timeout, dots)
		if migrateReply == "" {
			goto next
		}
		if err != nil {
			if strings.Contains(migrateReply, "BUSYKEY") {
				isBusy = true
			} else {
				isBusy = false
			}
			notServed = 0
			if !isBusy {
				/* Check if the slot is unassigned (not served) in the
				 * source node's configuration. */
				servedBy = clusterManagerGetSlotOwner(source, int64(slot), &getOwnerErr)
				if servedBy == nil {
					if getOwnerErr == nil {
						notServed = 1
					} else {
						clusterManagerPrintReplyError(source, getOwnerErr)
					}
				}
			}
			/* Try to handle errors. */
			if isBusy || intToBool(notServed) {
				/* If the key's slot is not served, try to assign slot
				 * to the target node. */
				if intToBool(doFix) && intToBool(notServed) {
					clusterManagerLogWarn("*** Slot was not served, setting owner to node %s:%d.", target.Ip, target.Port)
					clusterManagerSetSlot(source, target, slot, "node", nil)
				}
				/* If the key already exists in the target node (BUSYKEY),
				 * check whether its value is the same in both nodes.
				 * In case of equal values, retry migration with the
				 * REPLACE option.
				 * In case of different values:
				 *  - If the migration is requested by the fix command, stop
				 *    and warn the user.
				 *  - In other cases (ie. reshard), proceed only if the user
				 *    launched the command with the --cluster-replace option.*/
				if isBusy {
					clusterManagerLogWarn("*** Target key exists")
					if !intToBool(doReplace) {
						clusterManagerLogWarn("*** Checking key values on both nodes...")
						success = clusterManagerCompareKeysValues(source, target, reply, diffs)
						if !intToBool(success) {
							clusterManagerLogErr("*** Value check failed!")
							goto next
						}
						if len(diffs) > 0 {
							success = 0
							clusterManagerLogErr("*** Found %d key(s) in both source node and target node having different values.\n "+
								"    Source node: %s:%d\n"+
								"    Target node: %s:%d\n"+
								"    Keys(s):\n", len(diffs), source.Ip, source.Port, target.Ip, target.Port)
							for _, k := range diffs {
								clusterManagerLogErr("    - %s", k)
							}
							clusterManagerLogErr("Please fix the above key(s) manually and try again or relaunch the command \n" +
								"with --cluster-replace option to force key overriding.")
							goto next
						}
					}
					clusterManagerLogWarn("*** Replacing target keys...")
				}
				migrateReply = ""
				migrateReply, err = clusterManagerMigrateKeysInReply(source, target, reply, 1, timeout, "")
				if strings.ToLower(migrateReply) == "ok" {
					success = 1
				} else {
					success = 0
				}
			} else {
				success = 0
			}
		}

		if !intToBool(success) {
			clusterManagerPrintReplyError(source, err)
			goto next
		}
		if intToBool(verbose) {
			// fmt.Printf(dots)
		}
	next:
		if !intToBool(success) {
			break
		}
	}

	return 0
}

func clusterManagerCompareKeysValues(n1 *ClusterManagerNode, n2 *ClusterManagerNode, keysReply interface{}, diffs []interface{}) int {
	// 这个后面再看
	return 1
}

/* clusterManagerGetSlotOwner Get the node the slot is assigned to from the point of view of node *n.
 * If the slot is unassigned or if the reply is an error, return NULL.
 * Use the **err argument in order to check wether the slot is unassigned
 * or the reply resulted in an error. */
func clusterManagerGetSlotOwner(n *ClusterManagerNode, slot int64, err *error) *ClusterManagerNode {
	if slot > 0 && slot < ClusterManagerSlots {

	} else {
		return nil
	}
	var owner *ClusterManagerNode
	var reply interface{}
	var element []*clusterManagerSlots
	reply, *err = n.Context.Conn.Do("CLUSTER", "SLOTS")
	element = clusterManagerRelay(reply)
	for _, r := range element {
		if slot < r.From || slot > r.To {
			continue
		}
		for _, name := range r.Node {
			owner = clusterManagerNodeByName(&name.Name)
			if owner != nil {
				return owner
			}
			if owner == nil {
				for _, nd := range clusterManager.Nodes {
					if strings.Compare(nd.Ip, name.Ip) == 0 && name.Port == int64(nd.Port) {
						owner = nd
						break
					}
				}
			}
			if owner != nil {
				break
			}
		}
		if owner != nil {
			break
		}
	}
	return owner
}

type clusterManagerSlotNode struct {
	Ip   string
	Port int64
	Name string
}
type clusterManagerSlots struct {
	From int64
	To   int64
	Node []*clusterManagerSlotNode
}

func clusterManagerRelay(reply interface{}) []*clusterManagerSlots {
	ip := make([]*clusterManagerSlots, 0)
	for _, n := range reply.([]interface{}) {
		sn := new(clusterManagerSlots)
		sn.From = n.([]interface{})[0].(int64)
		sn.To = n.([]interface{})[1].(int64)
		sn.Node = make([]*clusterManagerSlotNode, 0)
		for _, i := range n.([]interface{})[2:] {
			node := new(clusterManagerSlotNode)
			node.Ip = string(i.([]interface{})[0].([]uint8))
			node.Port = i.([]interface{})[1].(int64)
			node.Name = string(i.([]interface{})[2].([]uint8))
			sn.Node = append(sn.Node, node)
		}
		ip = append(ip, sn)
	}
	return ip
}

/* clusterManagerMigrateKeysInReply Migrate keys taken from reply->elements. It returns the reply from the
 * MIGRATE command, or NULL if something goes wrong. If the argument 'dots'
 * is not NULL, a dot will be printed for every migrated key. */
func clusterManagerMigrateKeysInReply(source *ClusterManagerNode, target *ClusterManagerNode, reply interface{}, replace int, timeout int, dots string) (string, error) {
	var argv = []interface{}{"MIGRATE"}
	argv = append(argv, []interface{}{target.Ip, target.Port, "", 0, timeout}...)
	if intToBool(replace) {
		argv = append(argv, "REPLACE")
	}
	if config.auth != "" {
		if config.user != "" {
			argv = append(argv, []interface{}{"AUTH2", config.user, config.auth}...)
		} else {
			argv = append(argv, []interface{}{"AUTH", config.auth}...)
		}
	}
	argv = append(argv, "KEYS")
	for _, key := range reply.([]interface{}) {
		value := string(key.([]uint8))
		argv = append(argv, value)
	}
	clusterManagerLogInfof(".")
	success, err := redis.String(source.Context.Conn.Do(argv[0].(string), argv[1:]...))
	if err != nil {
		return "", err
	}
	return success, err
}

// deleteSlice 删除指定元素。
func deleteSlice(a []int, elem int) []int {
	j := 0
	for _, v := range a {
		if v != elem {
			a[j] = v
			j++
		}
	}
	return a[:j]
}
func clusterManagerShowReshardTable(table []*clusterManagerReshardTableItem) {
	for _, item := range table {
		clusterManagerLogInfo("    Moving slot %d from %s", item.slot, item.source.Name)
	}
}
func clusterManagerComputeReshardTable(sources []*ClusterManagerNode, numSlots int) (moved []*clusterManagerReshardTableItem) {
	if moved == nil {
		moved = make([]*clusterManagerReshardTableItem, 0)
	}
	var srcCount int
	srcCount = len(sources)
	var i, j, totSlots int
	var sorted []*ClusterManagerNode
	sorted = make([]*ClusterManagerNode, srcCount)
	for _, node := range sources {
		totSlots += node.SlotsCount
		sorted[i] = node
		i++
	}
	sort.Slice(sorted, func(i, j int) bool {
		return intToBool(sorted[j].SlotsCount - sorted[i].SlotsCount)
	})

	// sort.Slice(sorted, clusterManagerSlotCountCompareDesc)
	for i = 0; i < srcCount; i++ {
		node := sorted[i]
		var n float64
		n = float64(numSlots) / float64(totSlots) * float64(node.SlotsCount)
		if i == 0 {
			n = math.Ceil(n)
		} else {
			n = math.Floor(n)
		}
		var max, count int
		max = int(n)
		for j = 0; j < ClusterManagerSlots; j++ {
			slot := intInSlice(node.Slots, j)
			if !slot {
				continue
			}
			if count >= max || len(moved) >= numSlots {
				break
			}
			var item *clusterManagerReshardTableItem
			item = new(clusterManagerReshardTableItem)
			item.source = node
			item.slot = j
			moved = append(moved, item)
			count++
		}

	}
	sorted = nil
	return moved
}

func clusterManagerSlotCountCompareDesc(i, j int) bool {
	return false
}
func clusterNodeForResharding(id string, target *ClusterManagerNode, raiseErr *int) (node *ClusterManagerNode) {
	var invalidNodeMsg string
	invalidNodeMsg = "*** The specified node (%s) is not known or not a master, please retry."
	node = clusterManagerNodeByName(&id)
	*raiseErr = 0
	if node == nil || intToBool(node.Flags&ClusterManagerFlagSlave) {
		clusterManagerLogErr(invalidNodeMsg, id)
		*raiseErr = 1
		return nil
	} else if target != nil {
		if !intToBool(strings.Compare(node.Name, target.Name)) {
			clusterManagerLogErr("*** It is not possible to use the target node as source node.")
			return nil
		}
	}
	return node
}

/* Used for the reshard table. */
type clusterManagerReshardTableItem struct {
	source *ClusterManagerNode
	slot   int
}
