package redis_cli

import (
	"github.com/gomodule/redigo/redis"
	"strings"
)

func clusterManagerCommandDeleteNode(argc *Usage) {
	var port int
	var ip string
	var err error
	getClusterHostFromCmdArgs(argc.DelNode, &ip, &port)
	var nodeId string
	nodeId = argc.NodeID
	clusterManagerLogInfo(">>> Removing node %s from cluster %s:%d", nodeId, ip, port)
	var refNode, node *ClusterManagerNode
	refNode = clusterManagerNewNode(ip, port)
	// Load cluster information
	err = clusterManagerLoadInfoFromNode(refNode, 0)
	if err != nil {
		clusterManagerPrintAuthError(refNode, err)
		return
	}
	// Check if the node exists and is not empty
	node = clusterManagerNodeByName(&nodeId)
	if node == nil {
		clusterManagerLogErr("[ERR] No such node ID %s", nodeId)
		return
	}
	if node.SlotsCount != 0 {
		clusterManagerLogErr("[ERR] Node %s:%d is not empty! Reshard data away and try again.", node.Ip, node.Port)
		return
	}
	// Send CLUSTER FORGET to all the nodes but the node to remove
	clusterManagerLogInfo(">>> Sending CLUSTER FORGET messages to the cluster...")
	var r string
	for _, n := range clusterManager.Nodes {
		if n == node {
			continue
		}
		if n.Replicate != "" && !intToBool(strings.Compare(n.Replicate, nodeId)) {
			master := clusterManagerNodeWithLeastReplicas()
			if master == nil {
				return
			}
			clusterManagerLogInfo(">>> %s:%d as replica of %s:%d", n.Ip, n.Port, master.Ip, master.Port)
			var r string
			r, err = redis.String(n.Context.Do("CLUSTER", "REPLICATE", master.Name))
			if err != nil {
				return
			}
			if strings.ToLower(r) != "ok" {
				return
			}
		}
		r, err = redis.String(n.Context.Do("CLUSTER", "FORGET", nodeId))
		if err != nil {
			return
		}
		if strings.ToLower(r) != "ok" {
			return
		}
	}
	/* Finally send CLUSTER RESET to the node. */
	clusterManagerLogInfo(">>> Sending CLUSTER RESET SOFT to the deleted node.")
	r, err = redis.String(node.Context.Do("CLUSTER", "RESET", "SOFT"))
	if err != nil {
		return
	}
}

/* clusterManagerNodeWithLeastReplicas This function returns the master
 * that has the least number of replicas
 * in the cluster. If there are multiple masters with the same smaller
 * number of replicas, one at random is returned. */
func clusterManagerNodeWithLeastReplicas() (node *ClusterManagerNode) {
	var lowestCount int
	for _, n := range clusterManager.Nodes {
		if intToBool(n.Flags & ClusterManagerFlagSlave) {
			continue
		}
		if node == nil || n.ReplicasCount < lowestCount {
			node = n
			lowestCount = n.ReplicasCount
		}
	}
	return
}
