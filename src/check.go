package redis_cli

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"
)

/* Used by clusterManagerFixSlotsCoverage */
var clusterManagerUncoveredSlots map[int][]*ClusterManagerNode

func clusterManagerCheckCluster(quiet int) error {
	var node *ClusterManagerNode
	node = clusterManager.Nodes[0]
	if node == nil {
		return fmt.Errorf("first node is null")
	}
	clusterManagerLogInfo(">>> Performing Cluster Check (using node %s:%d)", node.Ip, node.Port)
	var err error
	var consistent bool
	var doFix bool

	consistent = false
	doFix = intToBool(config.flags & ClusterManagerCmdFlagFix)
	if !intToBool(quiet) {
		clusterManagerShowNodes()
	}
	consistent = clusterManagerIsConfigConsistent()
	if !consistent {
		err = fmt.Errorf("[ERR] Nodes don't agree about configuration%s", "!")
		clusterManagerOnError(err)
	} else {
		clusterManagerLogOk("[OK] All Nodes agree about slots configuration.")
	}
	/* Check open slots */
	clusterManagerLogInfo(">>> Check for open slots...")
	var openSlots map[int]string
	var i int
	for _, n := range clusterManager.Nodes {
		if n.Migrating != nil {
			if openSlots == nil {
				openSlots = make(map[int]string)
			}
			errStr := fmt.Sprintf("[WARNING] Node %s:%d has slots in migrating state ", n.Ip, n.Port)
			i = 0
			for slots, _ := range n.Migrating {
				slot := slots
				openSlots[slot] = n.Migrating[slot]
				if i > 0 {
					errStr += fmt.Sprintf(",%d", slot)
				} else {
					errStr += fmt.Sprintf("%d", slot)
				}
				i++
			}
			errStr += "."
			clusterManagerLogErr(errStr)
		}
		if n.Importing != nil {
			if openSlots == nil {
				openSlots = make(map[int]string)
			}
			errStr := fmt.Sprintf("[WARNING] Node %s:%d has slots in importing state ", n.Ip, n.Port)
			i = 0
			for slots, _ := range n.Importing {
				slot := slots
				openSlots[slot] = n.Importing[slot]
				if i > 0 {
					errStr += fmt.Sprintf(",%d", slot)
				} else {
					errStr += fmt.Sprintf("%d", slot)
				}
				i++
			}
			errStr += "."
			clusterManagerLogErr(errStr)
		}

	}

	if openSlots != nil {
		errStr := "[WARNING] The following slots are open: "
		i = 0
		for entry, _ := range openSlots {
			slot := entry
			if i > 0 {
				errStr += fmt.Sprintf(",%d", slot)
			} else {
				errStr += fmt.Sprintf("%d", slot)
			}
			i++
		}
		errStr += "."
		clusterManagerLogErr(errStr)
		errStr = ""
		if doFix {
			/* Fix open slots. */
			for slot, _ := range openSlots {
				err := clusterManagerFixOpenSlot(slot)
				if err != nil {
					break
				}
			}
		}
	}

	clusterManagerLogInfo(">>> Check slots coverage...")
	var slots []int
	var coverage int
	slots = make([]int, 0)
	coverage, slots = clusterManagerGetCoveredSlots(slots)
	if coverage == ClusterManagerSlots {
		clusterManagerLogOk("[OK] All %d slots covered.", ClusterManagerSlots)
	} else {
		clusterManagerLogErr("[ERR] Not all %d slots are covered by Nodes.", ClusterManagerSlots)
		if doFix {
			// var dtype clusterManagerDictType
			var fixed int
			fixed, err = clusterManagerFixSlotsCoverage(slots)
			if fixed > 0 {
				// result = 1
			}
			if err == nil {
				clusterManagerPrintReplyError(node, err)
			}
		}
	}
	var searchMultipleOwners int
	searchMultipleOwners = config.flags & ClusterManagerCmdFlagCheckOwners
	if intToBool(searchMultipleOwners) {
		/* Check whether there are multiple owners, even when slots are
		 * fully covered and there are no open slots. */
		clusterManagerLogInfo(">>> Check for multiple slot owners...")
		var slot, slotsWithMultipleOwners int

		for ; slot < ClusterManagerSlots; slot++ {
			owners := make([]*ClusterManagerNode, 0)
			for _, n := range clusterManager.Nodes {
				if intToBool(n.Flags & ClusterManagerFlagSlave) {
					continue
				}
				if intInSlice(n.Slots, slot) {
					owners = append(owners, n)
				} else {
					/* Nodes having keys for the slot will be considered
					 * owners too. */
					var count int
					count, err = clusterManagerCountKeysInSlot(n, slot)
					if err != nil {
						clusterManagerPrintReplyError(node, err)
						return err
					}
					if count > 0 {
						owners = append(owners, n)
					}
				}
			}
			if len(owners) > 1 {
				clusterManagerLogErr("[WARNING] Slot %d has %d owners:", slot, len(owners))
				for _, n := range clusterManager.Nodes {
					clusterManagerLogErr("    %s:%d", n.Ip, n.Port)
				}
				slotsWithMultipleOwners++
				if doFix {
					err = clusterManagerFixMultipleSlotOwners(slot, owners)
					if err != nil {
						clusterManagerLogErr("Failed to fix multiple owners for slot %d", slot)
						break
					} else {
						slotsWithMultipleOwners--
					}
				}
			}
		}
		if slotsWithMultipleOwners == 0 {
			clusterManagerLogOk("[OK] No multiple owners found.")
		}
	}
	return err
}

func clusterManagerFixOpenSlot(slot int) error {
	var forceFix int
	forceFix = config.flags & ClusterManagerCmdFlagFixWithUnreachableMasters
	if clusterManager.UnreachableMasters > 0 && !intToBool(forceFix) {
		clusterManagerLogWarn("*** Fixing open slots with %d unreachable masters is dangerous: redis-cli will assume that slots about masters that are not reachable are not covered, and will try to reassign them to the reachable nodes. This can cause data loss and is rarely what you want to do. If you really want to proceed use the --cluster-fix-with-unreachable-masters option.", clusterManager.UnreachableMasters)
		os.Exit(1)
	}
	clusterManagerLogInfo(">>> Fixing open slot %d", slot)
	/* Try to obtain the current slot owner, according to the current
	 * nodes configuration. */
	var success int
	success = 1
	/* List of nodes claiming some ownership.
	it could be stating in the configuration
	   to have the node ownership, or just
	   holding keys for such slot. */
	var owners []*ClusterManagerNode
	owners = make([]*ClusterManagerNode, 0)
	var migrating []*ClusterManagerNode
	migrating = make([]*ClusterManagerNode, 0)
	var importing []*ClusterManagerNode
	importing = make([]*ClusterManagerNode, 0)
	var migratingStr string
	var importingStr string
	var err error
	var owner *ClusterManagerNode /* The obvious slot owner if any. */
	/* Iterate all the nodes, looking for potential owners of this slot. */
	for _, n := range clusterManager.Nodes {
		if intToBool(n.Flags & ClusterManagerFlagSlave) {
			continue
		}
		if intInSlice(n.Slots, slot) {
			owners = append(owners, n)
		} else {
			var r int
			r, err = redis.Int(n.Context.Conn.Do("CLUSTER", "COUNTKEYSINSLOT", slot))
			if err != nil {
				return err
			}
			if r > 0 {
				clusterManagerLogWarn("*** Found keys about slot %d in non-owner node %s:%d!", slot, n.Ip, n.Port)
				owners = append(owners, n)
			}
		}
	}
	/* If we have only a single potential owner for this slot,
	 * set it as "owner". */
	if len(owners) == 1 {
		owner = owners[0]
	}
	/* Scan the list of nodes again, in order to populate the
	 * list of nodes in importing or migrating state for
	 * this slot. */
	for _, n := range clusterManager.Nodes {
		if intToBool(n.Flags & ClusterManagerFlagSlave) {
			continue
		}
		var isMigrating, isImporting int
		if n.Migrating != nil {
			for key, _ := range n.Migrating {
				var migratingSlot int
				migratingSlot = key
				if migratingSlot == slot {
					var sep string
					if len(migrating) == 0 {
						sep = ""
					} else {
						sep = ","
					}
					migratingStr = fmt.Sprintf("%s%s:%d", sep, n.Ip, n.Port)
					migrating = append(migrating, n)
					isMigrating = 1
					break
				}
			}
		}
		if !intToBool(isMigrating) && n.Importing != nil {
			for key, _ := range n.Importing {
				var importingSlot int
				importingSlot = key
				if importingSlot == slot {
					var sep string
					if len(importing) == 0 {
						sep = ""
					} else {
						sep = ","
					}
					importingStr = fmt.Sprintf("%s%s:%d", sep, n.Ip, n.Port)
					importing = append(importing, n)
					isImporting = 1
					break
				}
			}
		}
		/* If the node is neither migrating nor importing and it's not
		 * the owner, then is added to the importing list in case
		 * it has keys in the slot. */
		if !intToBool(isMigrating) && !intToBool(isImporting) && n != owner {
			var r int
			r, err = redis.Int(n.Context.Conn.Do("CLUSTER", "COUNTKEYSINSLOT", slot))
			if err != nil {
				return err
			}
			if r > 0 {
				clusterManagerLogWarn("*** Found keys about slot %d in node %s:%d!", slot, n.Ip, n.Port)
				var sep string
				if len(importing) == 0 {
					sep = ""
				} else {
					sep = ","
				}
				importingStr = fmt.Sprintf("%s%s:%d", sep, n.Ip, n.Port)
				importing = append(importing, n)
			}

		}
	}
	if len(migratingStr) > 0 {
		migratingStr = strings.Trim(migratingStr, ",")
		clusterManagerLogInfo("Set as migrating in: %s", migratingStr)
	}
	if len(importingStr) > 0 {
		importingStr = strings.Trim(importingStr, ",")
		clusterManagerLogInfo("Set as importing in: %s", importingStr)
	}
	/* If there is no slot owner, set as owner the node with the biggest
	 * number of keys, among the set of migrating / importing nodes. */
	if owner == nil {
		clusterManagerLogInfo(">>> No single clear owner for the slot, selecting an owner by # of keys...")
		owner = clusterManagerGetNodeWithMostKeysInSlot(clusterManager.Nodes, slot, nil)
		// If we still don't have an owner, we can't fix it.
		if owner == nil {
			clusterManagerLogErr("[ERR] Can't select a slot owner.  Impossible to fix.")
			return fmt.Errorf("impossible to fix")
		}
		// Use ADDSLOTS to assign the slot.
		clusterManagerLogWarn("*** Configuring %s:%d as the slot owner", owner.Ip, owner.Port)
		err = clusterManagerClearSlotStatus(owner, slot)
		if err != nil {
			return err
		}
		err = clusterManagerSetSlotOwner(owner, slot, 0)
		if err != nil {
			return err
		}
		/* Since CLUSTER ADDSLOTS succeeded, we also update the slot
		 * info into the node struct, in order to keep it synced */
		if !intInSlice(owner.Slots, slot) {
			owner.Slots = append(owner.Slots, slot)
		}
		/* Make sure this information will propagate. Not strictly needed
		 * since there is no past owner, so all the other nodes will accept
		 * whatever epoch this node will claim the slot with. */
		err = clusterManagerBumpEpoch(owner)
		if err != nil {
			return err
		}
		/* Remove the owner from the list of migrating/importing
		 * nodes. */
		migrating = clusterManagerRemoveNodeFromList(migrating, owner)
		importing = clusterManagerRemoveNodeFromList(importing, owner)
	}
	/* If there are multiple owners of the slot, we need to fix it
	 * so that a single node is the owner and all the other nodes
	 * are in importing state. Later the fix can be handled by one
	 * of the base cases above.
	 *
	 * Note that this case also covers multiple nodes having the slot
	 * in migrating state, since migrating is a valid state only for
	 * slot owners. */
	if len(owners) > 1 {
		/* Owner cannot be NULL at this point, since if there are more owners,
		 * the owner has been set in the previous condition (owner == NULL). */
		if owner == nil {
			return fmt.Errorf("owner cannot be NULL at this point")
		}
		for _, n := range owners {
			if n == owner {
				continue
			}
			err = clusterManagerDelSlot(n, slot, 1)
			if err != nil {
				return err
			}
			n.Slots = deleteSlice(n.Slots, slot)
			/* Assign the slot to the owner in the node 'n' configuration.' */
			success = clusterManagerSetSlot(n, owner, slot, "node", nil)
			if !intToBool(success) {
				return fmt.Errorf("owner set node error")
			}
			success = clusterManagerSetSlot(n, owner, slot, "importing", nil)
			if !intToBool(success) {
				return fmt.Errorf("owner importing node error")
			}
			/* Avoid duplicates. */
			importing = clusterManagerRemoveNodeFromList(importing, n)
			importing = append(importing, n)
			/* Ensure that the node is not in the migrating list. */
			migrating = clusterManagerRemoveNodeFromList(migrating, n)
		}
	}
	var moveOpts int
	moveOpts = ClusterManagerOptVerbose

	if len(migrating) == 1 && len(importing) == 1 {
		/* Case 1: The slot is in migrating state in one node, and in
		 *         importing state in 1 node. That's trivial to address. */
		var src *ClusterManagerNode
		var dst *ClusterManagerNode
		src = migrating[0]
		dst = importing[0]
		clusterManagerLogInfo(">>> Case 1: Moving slot %d from %s:%d to %s:%d", slot, src.Ip, src.Port, dst.Ip, dst.Port)
		moveOpts |= ClusterManagerOptUpdate
		success = clusterManagerMoveSlot(src, dst, slot, moveOpts, nil)
	} else if len(migrating) == 0 && len(importing) > 0 {
		/* Case 2: There are multiple nodes that claim the slot as importing,
		 * they probably got keys about the slot after a restart so opened
		 * the slot. In this case we just move all the keys to the owner
		 * according to the configuration. */
		clusterManagerLogInfo(">>> Case 2: Moving all the %d slot keys to its owner %s:%d", slot, owner.Ip, owner.Port)
		moveOpts |= ClusterManagerOptCold
		for _, n := range importing {
			if n == owner {
				continue
			}
			success = clusterManagerMoveSlot(n, owner, slot, moveOpts, nil)
			if !intToBool(success) {
				return fmt.Errorf("move slot error")
			}
			clusterManagerLogInfo(">>> Setting %d as STABLE in %s:%d", slot, n.Ip, n.Port)
			err = clusterManagerClearSlotStatus(n, slot)
			if err != nil {
				return err
			}
		}
		/* Since the slot has been moved in "cold" mode, ensure that all the
		 * other nodes update their own configuration about the slot itself. */
		for _, n := range clusterManager.Nodes {
			if n == owner {
				continue
			}
			if intToBool(n.Flags & ClusterManagerFlagSlave) {
				continue
			}
			success = clusterManagerSetSlot(n, owner, slot, "NODE", nil)
			if !intToBool(success) {
				return fmt.Errorf("set slot error")
			}
		}
	} else if len(migrating) == 1 && len(importing) > 1 {
		/* Case 3: The slot is in migrating state in one node but multiple
		 * other nodes claim to be in importing state and don't have any key in
		 * the slot. We search for the importing node having the same ID as
		 * the destination node of the migrating node.
		 * In that case we move the slot from the migrating node to this node and
		 * we close the importing states on all the other importing nodes.
		 * If no importing node has the same ID as the destination node of the
		 * migrating node, the slot's state is closed on both the migrating node
		 * and the importing nodes. */
		var tryToFix int
		tryToFix = 1
		var src, dst *ClusterManagerNode
		src = migrating[0]
		var targetId string
		for migratingSlot, id := range src.Migrating {
			if migratingSlot == slot {
				targetId = id
				break
			}
		}
		if targetId == "" {
			return fmt.Errorf("found node id error")
		}
		for _, n := range importing {
			var count int
			count, err = clusterManagerCountKeysInSlot(n, slot)
			if err != nil {
				return err
			}
			if count > 0 {
				tryToFix = 0
				break
			}
			if strings.Compare(n.Name, targetId) == 0 {
				dst = n
			}
		}
		if !intToBool(tryToFix) {
			clusterManagerLogErr("[ERR] Sorry, redis-cli can't fix this slot yet (work in progress). Slot is set as migrating in %s, as importing in %s, owner is %s:%d", migratingStr,
				importingStr, owner.Ip, owner.Port)
			return fmt.Errorf("redis-cli can't fix this slot yet (work in progress)")
		}
		if dst != nil {
			clusterManagerLogInfo(">>> Case 3: Moving slot %d from %s:%d to %s:%d and closing it on all the other importing nodes.", slot, src.Ip, src.Port, dst.Ip, dst.Port)
			/* Move the slot to the destination node. */
			success = clusterManagerMoveSlot(src, dst, slot, moveOpts, nil)
			if !intToBool(success) {
				return fmt.Errorf("move slot error")
			}
			/* Close slot on all the other importing nodes. */
			for _, n := range importing {
				if dst == n {
					continue
				}
				err = clusterManagerClearSlotStatus(n, slot)
				if err != nil {
					return err
				}
			}
		} else {
			clusterManagerLogInfo(">>> Case 3: Closing slot %d on both migrating and importing nodes.", slot)
			/* Close the slot on both the migrating node and the importing
			 * nodes. */
			err = clusterManagerClearSlotStatus(src, slot)
			if err != nil {
				return err
			}
		}
	} else {
		var tryToCloseSlot bool
		tryToCloseSlot = len(importing) == 0 && len(migrating) == 1
		if tryToCloseSlot {
			var n *ClusterManagerNode
			n = migrating[0]
			if owner == nil || owner != n {
				var r [][]byte
				r, err = redis.ByteSlices(n.Context.Conn.Do("CLUSTER", "GETKEYSINSLOT", slot, 10))
				if err != nil {
					return err
				}
				tryToCloseSlot = len(r) == 0
			}
		}
		/* Case 4: There are no slots claiming to be in importing state, but
		 * there is a migrating node that actually don't have any key or is the
		 * slot owner. We can just close the slot, probably a reshard
		 * interrupted in the middle. */
		if tryToCloseSlot {
			var n *ClusterManagerNode
			n = migrating[0]
			clusterManagerLogInfo(">>> Case 4: Closing slot %d on %s:%d", slot, n.Ip, n.Port)
			_, err = redis.String(n.Context.Conn.Do("CLUSTER", "SETSLOT", slot, "STABLE"))
			if err != nil {
				return err
			}
		} else {
			clusterManagerLogErr("[ERR] Sorry, redis-cli can't fix this slot yet (work in progress). Slot is set as migrating in %s, as importing in %s, owner is %s:%d", migratingStr,
				importingStr, owner.Ip, owner.Port)
			return fmt.Errorf("work in progress")
		}
	}
	return nil
}

func clusterManagerDelSlot(node *ClusterManagerNode, slot int, ignoreUnassignedErr int) error {
	// var reply string
	var success int
	var err error
	_, err = redis.String(node.Context.Conn.Do("CLUSTER", "DELSLOTS", slot))
	if err != nil && intToBool(ignoreUnassignedErr) {
		var assignedTo *ClusterManagerNode
		var getOwnerErr error
		assignedTo = clusterManagerGetSlotOwner(node, int64(slot), &getOwnerErr)
		if assignedTo != nil {
			if getOwnerErr == nil {
				success = 1
			} else {
				clusterManagerPrintReplyError(node, getOwnerErr)
			}
		}
	}
	if !intToBool(success) && err != nil {
		clusterManagerPrintReplyError(node, err)
	}
	return err
}

func clusterManagerRemoveNodeFromList(nodeList []*ClusterManagerNode, node *ClusterManagerNode) (nodeLists []*ClusterManagerNode) {
	for _, ln := range nodeList {
		if node == ln {
			continue
		}
		nodeLists = append(nodeLists, ln)
	}
	return
}

func clusterManagerBumpEpoch(node *ClusterManagerNode) error {
	_, err := node.Context.Conn.Do("CLUSTER", "BUMPEPOCH")
	return err
}

func clusterManagerSetSlotOwner(owner *ClusterManagerNode, slot int, doClear int) (err error) {
	err = clusterManagerStartTransaction(owner)
	if err != nil {
		return err
	}
	/* Ensure the slot is not already assigned. */
	err = clusterManagerDelSlot(owner, slot, 1)
	if err != nil {
		return err
	}
	/* Add the slot and bump epoch. */
	err = clusterManagerAddSlot(owner, slot)
	if err != nil {
		return err
	}
	if intToBool(doClear) {
		err = clusterManagerClearSlotStatus(owner, slot)
		if err != nil {
			return err
		}
	}
	err = clusterManagerBumpEpoch(owner)
	if err != nil {
		return err
	}
	err = clusterManagerExecTransaction(owner, clusterManagerOnSetOwnerErr)
	return err
}

/* Callback used by clusterManagerSetSlotOwner transaction. It should ignore
 * errors except for ADDSLOTS errors.
 * Return 1 if the error should be ignored. */
func clusterManagerOnSetOwnerErr(reply []interface{}, n *ClusterManagerNode, bulkIdx int) bool {
	/* Only raise error when ADDSLOTS fail (bulk_idx == 1). */
	return bulkIdx != 1
}

/* Call EXEC command on a cluster node. */
func clusterManagerExecTransaction(node *ClusterManagerNode, onerror func(reply []interface{}, n *ClusterManagerNode, bulkIdx int) bool) (err error) {
	var reply interface{}
	reply, err = node.Context.Conn.Do("EXEC")
	if err != nil {
		return err
	}
	if onerror(reply.([]interface{}), node, 0) {
		return nil
	} else {
		return fmt.Errorf("exec multi transaction error")
	}
}

func clusterManagerAddSlot(node *ClusterManagerNode, slot int) (err error) {
	_, err = node.Context.Conn.Do("CLUSTER", "ADDSLOTS", slot)
	return err
}

/* Call MULTI command on a cluster node. */
func clusterManagerStartTransaction(node *ClusterManagerNode) error {
	_, err := node.Context.Conn.Do("MULTI")
	return err
}

func clusterManagerClearSlotStatus(node *ClusterManagerNode, slot int) error {
	_, err := node.Context.Conn.Do("CLUSTER", "SETSLOT", slot, "STABLE")
	return err
}

/* clusterManagerGetNodeWithMostKeysInSlot Return the node, among 'nodes' with the greatest number of keys
 * in the specified slot. */
func clusterManagerGetNodeWithMostKeysInSlot(nodes []*ClusterManagerNode, slot int, err error) *ClusterManagerNode {
	var node *ClusterManagerNode
	var numKeys int
	for _, n := range nodes {
		if intToBool(n.Flags & ClusterManagerFlagSlave) {
			continue
		}
		var r int
		r, err = redis.Int(n.Context.Conn.Do("CLUSTER", "COUNTKEYSINSLOT", slot))
		if err != nil {
			clusterManagerPrintReplyError(n, err)
			node = nil
			break
		}
		if r > numKeys || node == nil {
			numKeys = r
			node = n
		}
	}
	return node
}

/* clusterManagerGetCoveredSlots Check the slots coverage of the cluster. The 'all_slots' argument must be
 * and array of 16384 bytes. Every covered slot will be set to 1 in the
 * 'all_slots' array. The function returns the total number if covered slots.*/
func clusterManagerGetCoveredSlots(allSlots []int) (int, []int) {
	if clusterManager.Nodes == nil {
		return 0, nil
	}
	var totSlots, i int
	for _, node := range clusterManager.Nodes {
		for i = 0; i < ClusterManagerSlots; i++ {
			if intInSlice(node.Slots, i) && !intInSlice(allSlots, i) {
				allSlots = append(allSlots, i)
				totSlots++
			}

		}
	}
	return totSlots, allSlots
}

func clusterManagerFixSlotsCoverage(allSlots []int) (int, error) {
	var forceFix int
	forceFix = config.flags & ClusterManagerCmdFlagFixWithUnreachableMasters
	if clusterManager.UnreachableMasters > 0 && !intToBool(forceFix) {
		clusterManagerLogWarn("*** Fixing slots coverage with %d unreachable masters is dangerous: redis-cli will assume that slots about masters that are not reachable are not covered, and will try to reassign them to the reachable nodes. This can cause data loss and is rarely what you want to do. If you really want to proceed use the --cluster-fix-with-unreachable-masters option.", clusterManager.UnreachableMasters)
		os.Exit(1)
	}
	var i, fixed int
	var none, single, multi []int
	clusterManagerLogInfo(">>> Fixing slots coverage...")
	for i = 0; i < ClusterManagerSlots; i++ {
		covered := intInSlice(allSlots, i)
		if !covered {
			slot := i
			slotNodes := make([]*ClusterManagerNode, 0)
			var slotNodesStr string
			for _, n := range clusterManager.Nodes {
				if intToBool(n.Flags&ClusterManagerFlagSlave) || n.Replicate != "" {
					continue
				}
				reply, err := redis.ByteSlices(n.Context.Conn.Do("CLUSTER", "GETKEYSINSLOT", i, 1))
				if err != nil {
					fixed = -1
					return fixed, err
				}
				if len(reply) > 0 {
					slotNodes = append(slotNodes, n)
					if len(slotNodes) > 1 {
						slotNodesStr += ", "
					}
					slotNodesStr += fmt.Sprintf("%s:%d", n.Ip, n.Port)
				}
			}
			if clusterManagerUncoveredSlots == nil {
				clusterManagerUncoveredSlots = make(map[int][]*ClusterManagerNode, 0)
			}
			clusterManagerUncoveredSlots[slot] = slotNodes
		}
	}
	/* For every slot, take action depending on the actual condition:
	 * 1) No node has keys for this slot.
	 * 2) A single node has keys for this slot.
	 * 3) Multiple nodes have keys for this slot. */
	none = make([]int, 0)
	single = make([]int, 0)
	multi = make([]int, 0)
	for slot, nodes := range clusterManagerUncoveredSlots {
		switch len(nodes) {
		case 1:
			none = append(none, slot)
			break
		case 2:
			single = append(single, slot)
			break
		default:
			multi = append(multi, slot)
			break
		}
	}
	/* we want explicit manual confirmation from users for all the fix cases */
	var ignoreForce int
	ignoreForce = 1
	/*  Handle case "1": keys in no node. */
	if len(none) > 0 {
		clusterManagerLogInfo("The following uncovered slots have no keys across the cluster:")
		clusterManagerPrintSlotsList(none)
		if intToBool(confirmWithYes("Fix these slots by covering with a random node?", ignoreForce)) {
			for _, slot := range none {
				var n *ClusterManagerNode
				n = clusterManagerNodeMasterRandom()
				clusterManagerLogInfo(">>> Covering slot %d with %s:%d", slot, n.Ip, n.Port)
				err := clusterManagerSetSlotOwner(n, slot, 0)
				if err != nil {
					fixed = -1
					return fixed, err
				}
				/* Since CLUSTER ADDSLOTS succeeded, we also update the slot
				 * info into the node struct, in order to keep it synced */
				n.Slots = append(n.Slots, slot)
				fixed++
			}
		}
	}
	/*  Handle case "2": keys only in one node. */
	if len(single) > 0 {
		clusterManagerLogInfo("The following uncovered slots have keys in just one node:")
		clusterManagerPrintSlotsList(single)
		if intToBool(confirmWithYes("Fix these slots by covering with those nodes?", ignoreForce)) {
			for _, slot := range single {
				if _, ok := clusterManagerUncoveredSlots[slot]; ok {
					nodes := clusterManagerUncoveredSlots[slot]
					n := nodes[0]
					if n != nil {
						fixed = -1
						return fixed, fmt.Errorf("uncovered slot node is null")
					}
					clusterManagerLogInfo(">>> Covering slot %d with %s:%d", slot, n.Ip, n.Port)
					err := clusterManagerSetSlotOwner(n, slot, 0)
					if err != nil {
						fixed = -1
						return fixed, err
					}
					/* Since CLUSTER ADDSLOTS succeeded, we also update the slot
					 * info into the node struct, in order to keep it synced */
					n.Slots = append(n.Slots, slot)
					fixed++
				} else {
					fixed = -1
					return fixed, fmt.Errorf("not found uncovered slot")
				}
			}
		}
	}
	/* Handle case "3": keys in multiple nodes. */
	if len(multi) > 0 {
		clusterManagerLogInfo("The following uncovered slots have keys in multiple nodes:")
		clusterManagerPrintSlotsList(multi)
		if intToBool(confirmWithYes("Fix these slots by moving keys into a single node?", ignoreForce)) {
			for _, slot := range multi {
				if _, ok := clusterManagerUncoveredSlots[slot]; ok {
					nodes := clusterManagerUncoveredSlots[slot]
					target := clusterManagerGetNodeWithMostKeysInSlot(nodes, slot, nil)
					if target == nil {
						fixed = -1
						return fixed, fmt.Errorf("get node most key in slot error")
					}
					clusterManagerLogInfo(">>> Covering slot %d moving keys to %s:%d", slot, target.Ip, target.Port)
					err := clusterManagerSetSlotOwner(target, slot, 1)
					if err != nil {
						fixed = -1
						return fixed, err
					}
					/* Since CLUSTER ADDSLOTS succeeded, we also update the slot
					 * info into the node struct, in order to keep it synced */
					target.Slots = append(target.Slots, slot)
					for _, src := range nodes {
						if src == target {
							continue
						}
						/* Assign the slot to target node in the source node. */
						if !intToBool(clusterManagerSetSlot(src, target, slot, "NODE", nil)) {
							fixed = -1
						}
						if fixed < 0 {
							return fixed, fmt.Errorf("target node in the source node error")
						}
						/* Set the source node in 'importing' state
						 * (even if we will actually migrate keys away)
						 * in order to avoid receiving redirections
						 * for MIGRATE. */
						if !intToBool(clusterManagerSetSlot(src, target, slot, "IMPORTING", nil)) {
							fixed = -1
						}
						if fixed < 0 {
							return fixed, fmt.Errorf("importing slot error")
						}
						opts := ClusterManagerOptVerbose | ClusterManagerOptCold
						if !intToBool(clusterManagerMoveSlot(src, target, slot, opts, nil)) {
							fixed = -1
						}
						if fixed < 0 {
							return fixed, fmt.Errorf("move slot error")
						}
						err = clusterManagerClearSlotStatus(src, slot)
						if err != nil {
							fixed = -1
							return fixed, err
						}
					}
					fixed++
				} else {
					fixed = -1
					return fixed, fmt.Errorf("not found uncovered slot")
				}
			}
		}
	}
	return fixed, nil
}

func clusterManagerNodeMasterRandom() *ClusterManagerNode {
	var masterCount int
	var idx int
	for _, n := range clusterManager.Nodes {
		if intToBool(n.Flags & ClusterManagerFlagSlave) {
			continue
		}
		masterCount++
	}
	rand.Seed(time.Now().Unix())
	idx = rand.Int() % masterCount
	for _, n := range clusterManager.Nodes {
		if intToBool(n.Flags & ClusterManagerFlagSlave) {
			continue
		}
		if !intToBool(idx) {
			return n
		}
		idx--
	}
	/* Can not be reached */
	return nil
}

func clusterManagerPrintSlotsList(slots []int) {
	var n *ClusterManagerNode
	n = new(ClusterManagerNode)
	for _, slot := range slots {
		if slot >= 0 && slot < ClusterManagerSlots {
			n.Slots = append(n.Slots, slot)
		}
	}
	nodesList := clusterManagerNodeSlotsString(n)
	clusterManagerLogInfo(nodesList)
}

func clusterManagerCountKeysInSlot(node *ClusterManagerNode, slot int) (int, error) {
	var count int
	var err error
	count, err = redis.Int(node.Context.Conn.Do("CLUSTER", "COUNTKEYSINSLOT", slot))
	return count, err
}

func clusterManagerFixMultipleSlotOwners(slot int, owners []*ClusterManagerNode) (err error) {
	clusterManagerLogInfo(">>> Fixing multiple owners for slot %d...", slot)
	if len(owners) > 1 {
		return fmt.Errorf("multiple owner node")
	}
	var owner *ClusterManagerNode
	owner = clusterManagerGetNodeWithMostKeysInSlot(owners, slot, nil)
	if owner == nil {
		owner = owners[0]
	}
	clusterManagerLogInfo(">>> Setting slot %d owner: %s:%d", slot, owner.Ip, owner.Port)
	/* Set the slot owner. */
	err = clusterManagerSetSlotOwner(owner, slot, 0)
	if err != nil {
		return err
	}
	/* Update configuration in all the other master nodes by assigning the slot
	 * itself to the new owner, and by eventually migrating keys if the node
	 * has keys for the slot. */
	for _, n := range clusterManager.Nodes {
		if n == owner {
			continue
		}
		if intToBool(n.Flags & ClusterManagerFlagSlave) {
			continue
		}
		var count int
		count, err = clusterManagerCountKeysInSlot(n, slot)
		if err != nil {
			return err
		}
		if !(count > 0) {
			break
		}
		err = clusterManagerDelSlot(n, slot, 1)
		if err != nil {
			return err
		}
		if !intToBool(clusterManagerSetSlot(n, owner, slot, "node", nil)) {
			return fmt.Errorf("set %d slot error", slot)
		}
		if count > 0 {
			var opts int
			opts = ClusterManagerOptVerbose | ClusterManagerOptCold
			if !intToBool(clusterManagerMoveSlot(n, owner, slot, opts, nil)) {
				break
			}
		}
	}
	return nil
}

/* clusterManagerOnError Add the error string to cluster_manager.errors and print it. */
func clusterManagerOnError(err error) {
	if clusterManager.Errors == nil {
		clusterManager.Errors = make([]error, 0)
	}
	clusterManager.Errors = append(clusterManager.Errors, err)
	clusterManagerLogErr(err.Error())
}

func clusterManagerShowNodes() {
	for _, node := range clusterManager.Nodes {
		info := clusterManagerNodeInfo(node, 0)
		clusterManagerLogInfo("%s", info)
	}
}

func clusterManagerIsConfigConsistent() bool {
	if len(clusterManager.Nodes) == 0 {
		return intToBool(0)
	}
	var consistent bool
	// If the Cluster has only one node, it's always consistent
	if len(clusterManager.Nodes) <= 1 {
		consistent = true
	}
	if consistent {
		return intToBool(1)
	}
	var firstCfg string
	for _, node := range clusterManager.Nodes {
		cfg := clusterManagerGetConfigSignature(node)
		if cfg == "" {
			consistent = false
			break
		}
		if firstCfg == "" {
			firstCfg = cfg
		} else {
			consistent = !intToBool(strings.Compare(firstCfg, cfg))
			if !consistent {
				break
			}
		}
	}
	return consistent
}

func clusterManagerGetConfigSignature(node *ClusterManagerNode) (signature string) {
	var nodeCount, i int
	var p string
	var nodeConfigs []string
	redisReply, err := node.Context.Do("CLUSTER", "NODES")
	if err != nil {
		return signature
	}
	lines := strings.Split(strings.Trim(redisReply, "\n"), "\n")
	for _, line := range lines {
		var nodeName string
		// var totSize int
		for i, p = range strings.Split(line, " ") {
			if i == 8 {
				break
			}
			token := p
			if i == 0 {
				nodeName = token
			}
			i++
		}
		if i != 8 {
			continue
		}
		if nodeName == "" {
			continue
		}
		remaining := len(line)
		if remaining == 0 {
			continue
		}
		/*var slots []int
		var c int*/
		i = 0
		var cfg string
		for _, p = range strings.Split(line, " ")[8:] {
			if strings.HasPrefix(p, "[") {
				continue
			}
			if i == 0 {
				cfg += nodeName + ":"
			}
			if i > 0 {
				cfg += ","
			}
			cfg += p
			i++
		}
		if cfg == "" {
			continue
		}
		nodeConfigs = append(nodeConfigs, cfg)
		nodeCount += 1
	}
	if nodeCount > 0 {
		for i = 0; i < nodeCount; i++ {
			if i > 0 {
				signature += "|"
			}
			signature += nodeConfigs[i]
		}
	}
	// 1dc2db8f6563c72a7fb7d815524bd01cc2d08ac7:0-5459|55382d2dd0bbd8dcaa9813f08835ba58d35daaba:16381-16383,5460-10922|cc28dac7407f01d2daac0dfbb4eb7f5c58ff6529:10923-16380
	qSort(&signature)
	return signature
}

func qSort(signature *string) {
	var m map[string]string
	if m == nil {
		m = make(map[string]string)
	}
	var s []string
	var t []string
	for _, n := range strings.Split(*signature, "|") {
		s = append(s, strings.Split(n, ":")[0])
		m[strings.Split(n, ":")[0]] = strings.Split(n, ":")[1]
	}
	sort.Strings(s)
	for _, y := range s {
		t = append(t, y)
		t = append(t, m[y])
	}
	var strFmt string
	for i := 0; i < len(t); i += 2 {
		if i > 0 {
			strFmt += "|"
		}
		strFmt += fmt.Sprintf("%s:%s", t[i], t[i+1])

	}
	*signature = strFmt
	return
}

func clusterManagerNodeInfo(node *ClusterManagerNode, indent int) string {
	var info string
	var spaces string
	for i := 0; i < indent; i++ {
		spaces += " "
	}
	if intToBool(indent) {
		info += spaces
	}
	var isMaster bool
	isMaster = !intToBool(node.Flags & ClusterManagerFlagSlave)
	var role string
	if isMaster {
		role = "M"
	} else {
		role = "S"
	}
	if intToBool(node.Dirty) && node.Replicate != "" {
		info += fmt.Sprintf("S: %s %s:%d", node.Name, node.Ip, node.Port)
	} else {
		slots := clusterManagerNodeSlotsString(node)
		flags := clusterManagerNodeFlagString(node)
		info += fmt.Sprintf("%s: %s %s:%d\n %s  slots:%s (%d slots) %s", role, node.Name, node.Ip, node.Port, spaces, slots, node.SlotsCount, flags)
	}
	if node.Replicate != "" {
		info += fmt.Sprintf("\n %s  replicates %s", spaces, node.Replicate)
	} else if intToBool(node.ReplicasCount) {
		info += fmt.Sprintf("\n %s  %d additional replica(s)", spaces, node.ReplicasCount)
	}
	return info
}

// containsInt Returns the index position of the int64 val in array
func containsInt(array []int, val int) bool {
	for i := 0; i < len(array); i++ {
		if array[i] == val {
			return true
		}
	}
	return false
}

/* clusterManagerNodeSlotsString Return a representable string of the node's slots */
func clusterManagerNodeSlotsString(node *ClusterManagerNode) string {
	var slots string
	var firstRangeIdx int
	var lastSlotIdx int
	var i int
	firstRangeIdx = -1
	lastSlotIdx = -1
	for i = 0; i < ClusterManagerSlots; i++ {
		hasSlot := containsInt(node.Slots, i)
		if hasSlot {
			if firstRangeIdx == -1 {
				if intToBool(len(slots)) {
					slots += ","
				}
				firstRangeIdx = i
				slots += fmt.Sprintf("[%d", i)
			}
			lastSlotIdx = i
		} else {
			if lastSlotIdx >= 0 {
				if firstRangeIdx == lastSlotIdx {
					slots += "]"
				} else {
					slots += fmt.Sprintf("-%d]", lastSlotIdx)
				}
			}
			lastSlotIdx = -1
			firstRangeIdx = -1
		}
	}
	if lastSlotIdx >= 0 {
		if firstRangeIdx == lastSlotIdx {
			slots += fmt.Sprintf("]")
		} else {
			slots += fmt.Sprintf("-%d]", lastSlotIdx)
		}
	}
	return slots
}

/* clusterManagerNodeFlagString Return a representable string of the node's flags */
func clusterManagerNodeFlagString(node *ClusterManagerNode) string {
	var flags string
	if len(node.FlagsStr) == 0 {
		return flags
	}
	var empty int
	empty = 1
	for _, flag := range node.FlagsStr {
		if strings.Compare(flag, "myself") == 0 {
			continue
		}
		if !intToBool(empty) {
			flags += ","
		}
		flags += flag
		empty = 0
	}
	return flags
}

func clusterManagerCommandCheck(argc *Usage) {
	var ip string
	var port int
	var err error
	if argc.Check != "" {
		getClusterHostFromCmdArgs(argc.Check, &ip, &port)
	} else if argc.Fix != "" {
		getClusterHostFromCmdArgs(argc.Fix, &ip, &port)
	} else {
		clusterManagerLogFatalf("[ERR] Invalid arguments")
	}

	node := clusterManagerNewNode(ip, port)
	err = clusterManagerLoadInfoFromNode(node, 0)
	if err != nil {
		clusterManagerPrintAuthError(node, err)
		return
	}
	clusterManagerShowClusterInfo()
	err = clusterManagerCheckCluster(0)
	if err != nil {
		clusterManagerLogFatalf(err.Error())
	}
}
