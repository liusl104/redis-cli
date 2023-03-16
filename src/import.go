package redis_cli

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"strconv"
)

func clusterManagerCommandImport(argc *Usage) {
	// var success int
	var port, srcPort int
	var ip, srcIp string
	// var invalidArgsMsg string
	getClusterHostFromCmdArgs(argc.Import, &ip, &port)
	if argc.ClusterFrom == "" {
		clusterManagerLogFatalf("[ERR] Option '--cluster-from' is required for subcommand 'import'.")
	}
	getClusterHostFromCmdArgs(argc.ClusterFrom, &srcIp, &srcPort)
	clusterManagerLogInfo(">>> Importing data from %s:%d to cluster %s:%d", srcIp, srcPort, ip, port)
	var refNode *ClusterManagerNode
	refNode = clusterManagerNewNode(ip, port)
	var err error
	err = clusterManagerLoadInfoFromNode(refNode, 0)
	if err != nil {
		clusterManagerPrintAuthError(refNode, err)
		return
	}
	err = clusterManagerCheckCluster(0)
	if err != nil {
		clusterManagerLogFatalf("[ERR] %s", err.Error())
	}
	var srcCtx redis.Conn
	srcCtx, err = redisConnect(srcIp, srcPort)
	if err != nil {
		clusterManagerLogFatalf("Could not connect to Redis at %s:%d: %s.", srcIp, srcPort, err.Error())
	}
	var fromUser string
	var fromPass string
	fromUser = argc.ClusterFromUser
	fromPass = argc.ClusterFromPass
	if !cliAuth(srcCtx, fromUser, fromPass) {
		return
	}
	var srcReply string
	srcReply, err = redis.String(srcCtx.Do("INFO"))
	if err != nil {
		clusterManagerLogFatalf("Source %s:%d replied with error:\n%s", srcIp, srcPort, err.Error())
	}
	var reply int
	reply, err = getLongInfoField(&srcReply, "cluster_enabled")
	if err != nil || intToBool(reply) {
		clusterManagerLogErr("[ERR] The source node should not be a cluster node.")
		return
	}
	var size int64
	size, err = redis.Int64(srcCtx.Do("DBSIZE"))
	if err != nil {
		clusterManagerLogErr("Source %s:%d replied with error:\n%s", srcIp, srcPort, err.Error())
	}
	clusterManagerLogWarn("*** Importing %d keys from DB 0\n", size)
	// Build a slot -> node map
	var slotsMap map[uint16]*ClusterManagerNode
	slotsMap = make(map[uint16]*ClusterManagerNode, 0)
	var i int
	for i = 0; i < ClusterManagerSlots; i++ {
		for _, n := range clusterManager.Nodes {
			if intToBool(n.Flags & ClusterManagerFlagSlave) {
				continue
			}
			if n.SlotsCount == 0 {
				continue
			}
			if intInSlice(n.Slots, i) {
				slotsMap[uint16(i)] = n
				break
			}
		}
	}
	var cmdFmt string
	cmdFmt = "MIGRATE"
	var cmd []interface{}
	var cmdExtend []interface{}
	cmdExtend = []interface{}{}
	if argc.Password != "" {
		if argc.User != "" {
			cmdExtend = append(cmdExtend, []interface{}{"AUTH2", argc.User, argc.Password}...)
			// cmdFmt += fmt.Sprintf(" AUTH2 %s %s", argc.User, argc.Password)
		} else {
			cmdExtend = append(cmdExtend, []interface{}{"AUTH", argc.Password}...)
			// cmdFmt += fmt.Sprintf(" AUTH %s", argc.Password)
		}
	}
	if argc.ClusterCopy {
		// cmdFmt += " COPY"
		cmdExtend = append(cmdExtend, "COPY")
	}
	if argc.ClusterReplace {
		cmdExtend = append(cmdExtend, "REPLACE")
		// cmdFmt += " REPLACE"
	}
	/* Use SCAN to iterate over the keys, migrating to the
	 * right node as needed. */
	var cursor int
	var timeout int
	cursor = -999
	timeout = config.timeout
	for cursor != 0 {
		if cursor < 0 {
			cursor = 0
		}
		var srcReplyResult interface{}
		srcReplyResult, err = srcCtx.Do("SCAN", cursor, "COUNT", 1000)
		cursor, err = strconv.Atoi(string(srcReplyResult.([]interface{})[0].([]uint8)))
		if err != nil {
			clusterManagerLogErr("Source %s:%d replied with error:\n%s", srcIp, srcPort, err.Error())
		}
		var keyContent []interface{}
		keyContent = srcReplyResult.([]interface{})[1].([]interface{})
		for _, kr := range keyContent {
			key := string(kr.([]uint8))
			var slot uint16
			slot = clusterManagerKeyHashSlot([]byte(key))
			var target *ClusterManagerNode
			target = slotsMap[slot]
			msg := fmt.Sprintf("Migrating %s to %s:%d: ", key, target.Ip, target.Port)
			// var r string
			// _, err = redis.String(srcCtx.Do(fmt.Sprintf(cmdFmt, target.Ip, target.Port, key, 0, timeout)))
			cmd = []interface{}{target.Ip, target.Port, key, 0, timeout}
			cmd = append(cmd, cmdExtend...)
			_, err = redis.String(srcCtx.Do(cmdFmt, cmd...))
			if err != nil {
				clusterManagerLogErr("%s Source %s:%d replied with error:\n%s", msg, srcIp, srcPort, err.Error())
				return
			}
			clusterManagerLogInfof(msg)
			clusterManagerLogOk("OK")
		}
	}
}

/* -----------------------------------------------------------------------------
 * Key space handling
 * -------------------------------------------------------------------------- */

/* clusterManagerKeyHashSlot We have 16384 hash slots. The hash slot of a given key is obtained
 * as the least significant 14 bits of the crc16 of the key.
 *
 * However if the key contains the {...} pattern, only the part between
 * { and } is hashed. This may be useful in the future to force certain
 * keys to be in the same node (assuming no resharding is in progress). */
func clusterManagerKeyHashSlot(key []byte) uint16 {
	var s, e int /* start-end indexes of { and } */
	var keyLen int
	keyLen = len(key)
	for s = 0; s < keyLen; s++ {
		if key[s] == '{' {
			break
		}
	}
	/* No '{' ? Hash the whole key. This is the base case. */
	if s == keyLen {
		return crc16(key) & 0x3FFF
	}
	/* '{' found? Check if we have the corresponding '}'. */
	for e = s + 1; e < keyLen; e++ {
		if key[e] == '}' {
			break
		}
	}
	/* No '}' or nothing between {} ? Hash the whole key. */
	if e == keyLen || e == s+1 {
		return crc16(key) & 0x3FFF
	}
	/* If we are here there is both a { and a } on its right. Hash
	 * what is in the middle between { and }. */

	return crc16(key) & 0x3FFF
}

func crc16(buf []byte) uint16 {
	var crc uint16 = 0
	var ch byte = 0
	size := len(buf)
	for i := 0; i < size; i++ {
		ch = byte(crc >> 12)
		crc <<= 4
		crc ^= crc16tab[ch^(buf[i]/16)]

		ch = byte(crc >> 12)
		crc <<= 4
		crc ^= crc16tab[ch^(buf[i]&0x0f)]
	}
	return crc
}
func intInSlice(haystack []int, needle int) bool {
	for _, e := range haystack {
		if e == needle {
			return true
		}
	}

	return false
}
func redisConnect(host string, port int) (redis.Conn, error) {
	conn, err := redis.Dial("tcp", fmt.Sprintf("%s:%d", host, port))

	if err != nil {
		return nil, err
	}
	return conn, nil
}

/* Send AUTH command to the server */
func cliAuth(ctx redis.Conn, user string, auth string) bool {
	var reply interface{}
	var err error
	if auth == "" {
		return RedisOk
	}
	if user == "" {
		reply, err = ctx.Do("AUTH", auth)
	} else {
		reply, err = ctx.Do("AUTH2", user, auth)
	}
	if reply == nil {
		clusterManagerLogErr("I/O error")
		return RedisErr
	}
	if err != nil {
		clusterManagerLogErr("AUTH failed:%s", err.Error())
		return RedisErr
	}
	return RedisOk
}
func reconnectingRedisCommand() {

}
