package redis_cli

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/siddontang/go-log/log"
	"net"
	"strconv"
	"strings"
)

/* clusterManagerLoadInfoFromNode Retrieves info about the cluster using argument 'node' as the starting
 * point. All Nodes will be loaded inside the cluster_manager.Nodes list.
 * Warning: if something goes wrong, it will free the starting node before
 * returning 0. */
func clusterManagerLoadInfoFromNode(node *ClusterManagerNode, opts int) error {
	err := clusterManagerNodeConnect(node)
	if err != nil {
		return err
	}
	opts |= ClusterManagerOptGetFriends
	isCluster, err := clusterManagerNodeIsCluster(node)
	if !intToBool(isCluster) {
		clusterManagerPrintNotClusterNodeError(node, err)
	}
	err = clusterManagerNodeLoadInfo(node, opts)
	if err != nil {
		return err
	}
	if clusterManager == nil {
		clusterManager = new(ClusterManager)
		clusterManager.Nodes = make([]*ClusterManagerNode, 0)
	}
	clusterManager.Nodes = append(clusterManager.Nodes, node)
	if node.Friends != nil {
		for _, friend := range node.Friends {
			if friend.Ip == "" || friend.Port == 0 {
				_ = invalidFriend(friend)
				continue
			}
			if friend.Context == nil {
				err = clusterManagerNodeConnect(friend)
				if err != nil {
					_ = invalidFriend(friend)
					continue
				}
				err = clusterManagerNodeLoadInfo(friend, 0)
				if err == nil {
					if intToBool(friend.Flags & (ClusterManagerFlagNoAddr | ClusterManagerFlagDisconnect | ClusterManagerFlagFail)) {
						_ = invalidFriend(friend)
						continue
					}
					clusterManager.Nodes = append(clusterManager.Nodes, friend)
				} else {
					log.Errorf("[ERR] Unable to load info for node %s:%d", friend.Ip, friend.Port)
					_ = invalidFriend(friend)

				}

			}
		}
	}
	for _, n := range clusterManager.Nodes {
		if n.Replicate != "" {
			master := clusterManagerNodeByName(&n.Replicate)
			if master == nil {
				log.Warnf("*** WARNING: %s:%d claims to be slave of unknown node ID %s.", n.Ip, n.Port, n.Replicate)
			} else {
				master.ReplicasCount++
			}
		}

	}

	return nil
}

/* Return the node with the specified name (ID) or NULL. */
func clusterManagerNodeByName(name *string) (found *ClusterManagerNode) {
	if clusterManager.Nodes == nil {
		return
	}
	var lcname string
	lcname = *name
	lcname = strings.ToLower(lcname)
	for _, n := range clusterManager.Nodes {
		if n.Name != "" && !intToBool(strings.Compare(n.Name, lcname)) {
			found = n
			break
		}
	}
	return
}

func invalidFriend(friend *ClusterManagerNode) error {
	if !intToBool(friend.Flags & ClusterManagerFlagSlave) {
		clusterManager.UnreachableMasters++
	}
	return nil
}

// getClusterHostFromCmdArgs 判断是否为合法IP地址
func getClusterHostFromCmdArgs(argc string, ip *string, port *int) {
	var err error
	if len(strings.Split(argc, ":")) != 2 {
		clusterManagerLogFatalf(ClusterManagerInvalidHostArg)
	}
	*ip = strings.Split(argc, ":")[0]
	address := net.ParseIP(*ip)
	if address == nil {
		clusterManagerLogFatalf(ClusterManagerInvalidHostArg)
	}
	if strings.Split(argc, ":")[1] == "" {
		clusterManagerLogFatalf(ClusterManagerInvalidHostArg)
	}
	*port, err = strconv.Atoi(strings.Split(argc, ":")[1])
	if err != nil {
		clusterManagerLogFatalf(ClusterManagerInvalidHostArg)
	}
	if *port < 1 || *port > 65535 {
		clusterManagerLogFatalf(ClusterManagerInvalidHostArg)
	}
}

func clusterManagerShowClusterInfo() {
	var masters int
	var keys int
	for _, node := range clusterManager.Nodes {
		if !intToBool(node.Flags & ClusterManagerFlagSlave) {
			// 判断是不是从库
			if node.Name == "" {
				continue
			}
			var replicas int
			var dbsize int
			dbsize = -1
			name := node.Name
			for _, ri := range clusterManager.Nodes {
				if ri == node || !intToBool(ri.Flags&ClusterManagerFlagSlave) {
					continue
				}
				if ri.Replicate != "" && !intToBool(strings.Compare(ri.Replicate, node.Name)) {
					replicas++
				}
			}
			redisReply, err := redis.Int(node.Context.Conn.Do("DBSIZE"))
			if err != nil {
				return
			}
			dbsize = redisReply
			if dbsize < 0 {
				clusterManagerLogFatalf("Node %s:%d replied with error: %s", node.Ip, node.Port, err.Error())
			}
			clusterManagerLogInfo("%s:%d (%s...) -> %d keys | %d slots | %d slaves.", node.Ip, node.Port, name[:8], dbsize, node.SlotsCount, replicas)
			masters++
			keys += dbsize
		}
	}
	clusterManagerLogOk("[OK] %d keys in %d masters.", keys, masters)
	keysPerSlot := float64(keys) / float64(ClusterManagerSlots)
	clusterManagerLogInfo("%.2f keys per slot on average.", keysPerSlot)

}

func clusterManagerCommandInfo(argc *Usage) {
	var ip string
	var port int
	var err error
	getClusterHostFromCmdArgs(argc.Info, &ip, &port)
	node := clusterManagerNewNode(ip, port)
	err = clusterManagerLoadInfoFromNode(node, 0)
	if err != nil {
		clusterManagerPrintAuthError(node, err)
		return
	}
	clusterManagerShowClusterInfo()
}

func clusterManagerNodeLoadInfo(node *ClusterManagerNode, opt int) error {
	redisReply, err := node.Context.Do("CLUSTER", "NODES")
	if err != nil {
		return err
	}
	getfriends := intToBool(opt & ClusterManagerOptGetFriends)
	lines := strings.Split(strings.Trim(redisReply, "\n"), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		var name string
		var addr string
		var flags string
		var masterId string
		var pingSent string
		var pingRecv string
		var configEpoch string
		var linkStatus string
		_ = linkStatus
		p := strings.Split(line, " ")
		name = p[0]
		addr = p[1]
		flags = p[2]
		masterId = p[3]
		pingSent = p[4]
		pingRecv = p[5]
		configEpoch = p[6]
		linkStatus = p[7]
		myself := strings.Contains(flags, "myself")
		var currentNode *ClusterManagerNode
		if myself {
			node.Flags |= ClusterManagerFlagMyself
			currentNode = node
			clusterManagerNodeResetSlots(node)
			for _, slotLine := range p[8:] {
				if strings.Contains(slotLine, "->-") { // Migrating
					slotMigrating := strings.TrimLeft(slotLine, "[")
					slotMigrating = strings.TrimRight(slotMigrating, "]")
					node.MigratingCount += 1
					if node.Migrating == nil {
						node.Migrating = make(map[int]string)
					}
					var slot int
					var dst string
					slot, _ = strconv.Atoi(strings.Split(slotMigrating, "->-")[0])
					dst = strings.Split(slotMigrating, "->-")[1]
					node.Migrating[slot] = dst

				} else if strings.Contains(slotLine, "-<-") { //Importing
					slotImporting := strings.TrimLeft(slotLine, "[")
					slotImporting = strings.TrimRight(slotImporting, "]")
					node.ImportingCount += 1
					if node.Importing == nil {
						node.Importing = make(map[int]string)
					}
					var slot int
					var src string
					slot, _ = strconv.Atoi(strings.Split(slotImporting, "-<-")[0])
					src = strings.Split(slotImporting, "-<-")[1]
					node.Importing[slot] = src

				} else if strings.Contains(slotLine, "-") {
					var start, stop int
					start, _ = strconv.Atoi(strings.Split(slotLine, "-")[0])
					stop, _ = strconv.Atoi(strings.Split(slotLine, "-")[1])
					node.SlotsCount += stop - (start - 1)
					for slot := start; slot <= stop; slot++ {
						node.Slots = append(node.Slots, slot)
					}
				} else {
					slot, err := strconv.Atoi(slotLine)
					if err != nil {
						return err
					}
					node.Slots = append(node.Slots, slot)
					node.SlotsCount++
				}
			}
			node.Dirty = 0
		} else if !getfriends {
			if !intToBool(node.Flags & ClusterManagerFlagMyself) {
				continue
			} else {
				break
			}
		} else {
			if addr == "" {
				clusterManagerLogFatalf("Error: invalid CLUSTER NODES reply")
			}
			c := strings.Split(addr, "@")
			if len(c) != 2 {
				return fmt.Errorf("invalid CLUSTER NODES reply")
			}
			var port int
			port, _ = strconv.Atoi(strings.Split(c[0], ":")[1])
			addr = strings.Split(c[0], ":")[0]
			currentNode = clusterManagerNewNode(addr, port)
			currentNode.Flags |= ClusterManagerFlagFriend
			node.Friends = append(node.Friends, currentNode)
		}
		if name != "" {
			currentNode.Name = name
		}
		if currentNode.FlagsStr != nil {
			currentNode.FlagsStr = []string{}
		}
		for _, flagV := range strings.Split(flags, ",") {
			if strings.Compare(flagV, "noaddr") == 0 {
				currentNode.Flags |= ClusterManagerFlagNoAddr
			} else if strings.Compare(flagV, "disconnected") == 0 {
				currentNode.Flags |= ClusterManagerFlagDisconnect
			} else if strings.Compare(flagV, "fail") == 0 {
				currentNode.Flags |= ClusterManagerFlagFail
			} else if strings.Compare(flagV, "slave") == 0 {
				currentNode.Flags |= ClusterManagerFlagSlave
				if masterId != "" {
					currentNode.Replicate = masterId
				}
			}
			currentNode.FlagsStr = append(currentNode.FlagsStr, flagV)
		}
		if configEpoch != "" {
			currentNode.CurrentEpoch, _ = strconv.Atoi(configEpoch)
		}
		if pingSent != "" {
			currentNode.PingSent, _ = strconv.Atoi(pingSent)
		}
		if pingRecv != "" {
			currentNode.PingRecv, _ = strconv.Atoi(pingRecv)
		}
		if !getfriends && myself {
			break
		}
	}

	return nil

}

func clusterManagerNewNode(ip string, port int) *ClusterManagerNode {
	node := new(ClusterManagerNode)
	node.Ip = ip
	node.Port = port
	node.Dirty = 0
	node.Flags = 0
	node.MigratingCount = 0
	node.ImportingCount = 0
	node.ReplicasCount = 0
	node.Weight = 1.0
	node.Balance = 0
	return node
}

func clusterManagerNodeConnect(node *ClusterManagerNode) error {
	var err error
	if node.Context != nil {
		node.Context = nil
	}
	node.Context, err = newRedisConn("tcp", node.Ip, node.Port, RedisUser, RedisPassword)
	if err != nil {
		// node.Context.Close()
		// log.Errorf("Could not connect to Redis at %s:%d: %s", node.Ip, node.Port, err.Error())
		return err
	}
	return nil
}

/* getLongInfoField Like the above function but automatically convert the result into
 * a long. On error (missing field) LONG_MIN is returned. */
func getLongInfoField(info *string, field string) (int, error) {
	value, err := getInfoField(info, field)
	if err != nil {
		return 0, err
	}
	if value == 0 {
		return 0, err
	}
	return value, nil
}

func getInfoField(info *string, field string) (int, error) {
	for _, line := range strings.Split(*info, "\n") {
		if strings.HasPrefix(line, field) {
			p, err := strconv.Atoi(strings.Trim(strings.Split(line, ":")[1], "\r"))
			return p, err
		}
	}
	return 0, fmt.Errorf("unknown error")
}

func clusterManagerNodeIsCluster(node *ClusterManagerNode) (int, error) {
	info, err := clusterManagerGetNodeRedisInfo(node)
	if err != nil {
		return 0, err
	}
	var isCluster int
	isCluster, err = getLongInfoField(&info, "cluster_enabled")
	return isCluster, err

}

func clusterManagerPrintNotClusterNodeError(node *ClusterManagerNode, err error) {
	if err != nil {
		clusterManagerLogFatalf("[ERR] Node %s:%d is not configured as a cluster node. %s", node.Ip, node.Port, err.Error())
	}
	clusterManagerLogFatalf("[ERR] Node %s:%d is not configured as a cluster node. ", node.Ip, node.Port)
}

/* clusterManagerGetNodeRedisInfo Call "INFO" redis command on the specified node and return the reply. */
func clusterManagerGetNodeRedisInfo(node *ClusterManagerNode) (string, error) {
	redisReply, err := node.Context.Do("info")
	if err != nil {
		return "", err
	}
	if redisReply == "" {
		return redisReply, fmt.Errorf("unknown error")
	}
	return redisReply, nil
}

func clusterManagerNodeResetSlots(node *ClusterManagerNode) {
	node.SlotsCount = 0
}
