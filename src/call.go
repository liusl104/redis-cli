package redis_cli

import "strings"

func clusterManagerCommandCall(argc *Usage) {
	var port int
	var ip string
	getClusterHostFromCmdArgs(argc.Call, &ip, &port)
	var refNode *ClusterManagerNode
	refNode = clusterManagerNewNode(ip, port)
	err := clusterManagerLoadInfoFromNode(refNode, 0)
	if err != nil {
		clusterManagerPrintAuthError(refNode, err)
		return
	}
	clusterManagerLogInfo(">>> Calling %s", argc.Command)
	for _, n := range clusterManager.Nodes {
		if argc.CLusterOnlyMasters && n.Replicate != "" {
			continue // continue if node is slave
		}
		if argc.CLusterOnlyReplicas && n.Replicate == "" {
			continue // continue if node is master
		}
		if n.Context == nil {
			continue
		}
		err = clusterManagerNodeConnect(n)
		if err != nil {
			continue
		}
		var command []interface{}
		for _, comm := range strings.Split(argc.Command, " ") {
			if comm == " " {
				continue
			}
			command = append(command, comm)
		}
		var reply string
		reply, err = n.Context.Do(command[0].(string), command[1:]...)
		if err != nil {
			clusterManagerLogErr("%s:%d: Failed!", n.Ip, n.Port)
		} else {
			clusterManagerLogInfo("%s:%d: %s", n.Ip, n.Port, reply)
		}
	}
	defer func() {
		for _, node := range clusterManager.Nodes {
			node.Context.Close()
		}
	}()
	return
}
