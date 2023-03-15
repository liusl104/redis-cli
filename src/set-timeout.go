package redis_cli

import "strings"

func clusterManagerCommandSetTimeout(argc *Usage) {
	var port int
	var ip string
	getClusterHostFromCmdArgs(argc.SetTimeout, &ip, &port)
	var timeout int
	timeout = argc.Milliseconds
	if timeout < 100 {
		clusterManagerLogErr("Setting a node timeout of less than 100 milliseconds is a bad idea.")
		return
	}
	var node *ClusterManagerNode
	node = clusterManagerNewNode(ip, port)
	var err error
	err = clusterManagerLoadInfoFromNode(node, 0)
	if err != nil {
		clusterManagerPrintAuthError(node, err)
		return
	}
	var okCount, errCount int
	clusterManagerLogInfo(">>> Reconfiguring node timeout in every cluster node...")
	for _, n := range clusterManager.Nodes {
		err = nil
		var reply string
		reply, err = n.Context.Do("CONFIG", "SET", "CLUSTER-NODE-TIMEOUT", timeout)
		if err != nil {
			clusterManagerLogErr("ERR setting node-timeout for %s:%d: %s", n.Ip, n.Port, err.Error())
			errCount++
			continue
		}
		err = nil
		reply, err = n.Context.Do("CONFIG", "REWRITE")
		if err != nil {
			errCount++
			continue
		}
		if strings.ToLower(reply) != "ok" {
			clusterManagerLogErr("ERR setting node-timeout for %s:%d: %s", n.Ip, n.Port, err.Error())
			errCount++
			continue
		}
		clusterManagerLogWarn("*** New timeout set for %s:%d", n.Ip, n.Port)
		okCount++
	}
	clusterManagerLogInfo(">>> New node timeout set. %d OK, %d ERR.", okCount, errCount)
}
