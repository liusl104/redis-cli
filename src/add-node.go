package redis_cli

import (
	"strings"
	"time"
)

func clusterManagerCommandAddNode(argc *Usage) {
	var reply string
	var err error
	var refIp, ip string
	var refPort, port int
	getClusterHostFromCmdArgs(argc.ExistNode, &refIp, &refPort)
	getClusterHostFromCmdArgs(argc.AddNode, &ip, &port)
	clusterManagerLogInfo(">>> Adding node %s:%d to cluster %s:%d", ip, port, refIp, refPort)
	// Check the existing cluster
	var refNode *ClusterManagerNode
	refNode = clusterManagerNewNode(refIp, refPort)
	err = clusterManagerLoadInfoFromNode(refNode, 0)
	if err != nil {
		clusterManagerPrintAuthError(refNode, err)
		return
	}
	err = clusterManagerCheckCluster(0)
	if err != nil {
		return
	}
	/* If --cluster-master-id was specified, try to resolve it now so that we
	 * abort before starting with the node configuration. */
	var masterNode *ClusterManagerNode
	if intToBool(config.flags & ClusterManagerCmdFlagSlave) {
		var masterId string
		masterId = config.masterId
		if masterId != "" {
			masterNode = clusterManagerNodeByName(&masterId)
			if masterNode == nil {
				clusterManagerLogErr("[ERR] No such master ID %s", masterId)
				return
			}
		} else {
			masterNode = clusterManagerNodeWithLeastReplicas()
			if masterNode == nil {
				return
			}
			clusterManagerLogInfo("Automatically selected master %s:%d", masterNode.Ip, masterNode.Port)
		}
	}
	// Add the new node
	var newNode *ClusterManagerNode
	newNode = clusterManagerNewNode(ip, port)
	// var added int
	err = clusterManagerNodeConnect(newNode)
	if err != nil {
		clusterManagerLogErr("[ERR] Sorry, can't connect to node %s:%d", ip, port)
		return
	}
	err = nil
	var isCluster int
	isCluster, err = clusterManagerNodeIsCluster(newNode)
	if !intToBool(isCluster) {
		if err != nil {
			clusterManagerPrintNotClusterNodeError(newNode, err)
			return
		}
	}
	err = clusterManagerNodeLoadInfo(newNode, 0)
	if err != nil {
		clusterManagerPrintReplyError(newNode, err)
		return
	}
	err = nil
	var isEmpty bool
	isEmpty = clusterManagerNodeIsEmpty(newNode, &err)
	if !isEmpty {
		clusterManagerPrintNotEmptyNodeError(newNode, err)
		return
	}
	var first *ClusterManagerNode
	first = clusterManager.Nodes[0]
	clusterManager.Nodes = append(clusterManager.Nodes, newNode)
	// added = 1
	// Send CLUSTER MEET command to the new node
	clusterManagerLogInfo(">>> Send CLUSTER MEET to node %s:%d to make it join the cluster.", ip, port)
	reply, err = newNode.Context.Do("CLUSTER", "MEET", first.Ip, first.Port)
	if err != nil {
		return
	}
	if strings.ToLower(reply) != "ok" {
		return
	}
	reply = ""
	err = nil
	/* Additional configuration is needed if the node is added as a slave. */
	if masterNode != nil {
		time.Sleep(time.Second * 1)
		clusterManagerWaitForClusterJoin()
		clusterManagerLogInfo(">>> Configure node as replica of %s:%d.", masterNode.Ip, masterNode.Port)
		reply, err = newNode.Context.Do("CLUSTER", "REPLICATE", masterNode.Name)
		if err != nil {
			return
		}
		if strings.ToLower(reply) != "ok" {
			return
		}
	}
	clusterManagerLogOk("[OK] New node added correctly.")
}
