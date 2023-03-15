package redis_cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func clusterManagerCommandBackup(argc *Usage) {
	var port int
	var noIssues int
	var success bool
	success = true
	var ip string
	var err error
	getClusterHostFromCmdArgs(argc.Backup, &ip, &port)
	var refNode *ClusterManagerNode
	refNode = clusterManagerNewNode(ip, port)
	err = clusterManagerLoadInfoFromNode(refNode, 0)
	if err != nil {
		clusterManagerPrintAuthError(refNode, err)
		return
	}
	var clusterErrorsCount int
	err = clusterManagerCheckCluster(0)
	if err != nil {
		clusterErrorsCount++
	} else {
		noIssues = 1
	}
	config.backupDir = argc.BackupDirectory
	var json string
	json = "[\n"
	var firstNode int
	for _, node := range clusterManager.Nodes {
		if !intToBool(firstNode) {
			firstNode = 1
		} else {
			json += ",\n"
		}
		nodeJson := clusterManagerNodeGetJSON(node, clusterErrorsCount)
		json += nodeJson
		if node.Replicate != "" {
			continue
		}
		clusterManagerLogInfo(">>> Node %s:%d -> Saving RDB...", node.Ip, node.Port)
		// getRDB(node)
	}
	json += "\n]"
	var jsonPath string
	jsonPath = config.backupDir
	/* TODO: check if backup_dir is a valid directory. */
	if !filepath.IsAbs(jsonPath) {
		jsonPath = filepath.Dir(jsonPath)
	}
	jsonPath = filepath.Join(jsonPath, "nodes.json")
	clusterManagerLogInfo("Saving cluster configuration to: %s", jsonPath)
	var out *os.File
	out, err = os.OpenFile(jsonPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		clusterManagerLogErr("Could not save nodes to: %s", jsonPath)
		success = false
		goto Cleanup
	}

	_, err = out.WriteString(json)
	defer func() {
		_ = out.Close()
	}()
Cleanup:
	if success {
		if !intToBool(noIssues) {
			clusterManagerLogWarn("*** Cluster seems to have some problems, please be aware of it if you're going to restore this backup.")
		}
		clusterManagerLogOk("[OK] Backup created into: %s", config.backupDir)
	} else {
		clusterManagerLogOk("[ERR] Failed to back cluster!")
	}
	return
}

func clusterManagerNodeGetJSON(node *ClusterManagerNode, errorCount int) string {
	var json string
	var replicate string
	if node.Replicate != "" {
		replicate = fmt.Sprintf("\"%s\"", node.Replicate)
	} else {
		replicate = "null"
	}
	slots := clusterManagerNodeSlotsString(node)
	flags := clusterManagerNodeFlagString(node)
	slots = strings.Replace(slots, "-", ",", -1)
	json = fmt.Sprintf("  {\n"+
		"    \"name\": \"%s\",\n"+
		"    \"host\": \"%s\",\n"+
		"    \"port\": %d,\n"+
		"    \"replicate\": %s,\n"+
		"    \"slots\": [%s],\n"+
		"    \"slots_count\": %d,\n"+
		"    \"flags\": \"%s\",\n"+
		"    \"current_epoch\": %d", node.Name, node.Ip, node.Port, replicate, slots, node.SlotsCount, flags, node.CurrentEpoch)
	if errorCount > 0 {
		json += fmt.Sprintf(",\n    \"cluster_errors\": %d", errorCount)
	}
	if node.MigratingCount > 0 && node.Migrating != nil {
		var migrating string
		for slot, dest := range node.Migrating {
			if len(migrating) > 0 {
				migrating += ","
			}
			migrating += fmt.Sprintf("\"%d\": \"%s\"", slot, dest)
		}
		if len(migrating) > 0 {
			json += fmt.Sprintf(",\n    \"migrating\": {%s}", migrating)
		}
	}
	if node.ImportingCount > 0 && node.Importing != nil {
		var importing string
		for slot, from := range node.Importing {
			if len(importing) > 0 {
				importing += ","
			}
			importing += fmt.Sprintf("\"%d\": \"%s\"", slot, from)
		}
		if len(importing) > 0 {
			json += fmt.Sprintf(",\n    \"importing\": {%s}", importing)
		}
	}
	json += "\n  }"
	return json
}

/* getRDB This function implements --rdb, so it uses the replication protocol in order
 * to fetch the RDB file from a remote server. */
func getRDB(node *ClusterManagerNode) {
	var fileName string
	var s *RedisGo
	if node != nil {
		if node.Context == nil {
			return
		}
		fileName = clusterManagerGetNodeRDBFilename(node)
		s = node.Context
	} else {
		s = new(RedisGo)
		fileName = config.rdbFileName
	}
	var eofMark = [RdbEofMarkSize]byte{}
	var lastBytes = [RdbEofMarkSize]byte{}
	var useMark int
	var payLoad uint64
	payLoad = sendSync(s, eofMark)
	var buf = [4096]byte{}
	if payLoad == 0 {
		payLoad = UllongMax
		useMark = 1
		clusterManagerLogErr("SYNC with master, discarding bytes of bulk transfer until EOF marker...")
	} else {
		clusterManagerLogErr("SYNC with master, discarding %d bytes of bulk transfer...", payLoad)
	}
	/* Discard the payload. */
	for payLoad > 0 {
	}
	print(fileName)
	fmt.Println(useMark, buf, lastBytes)
}

/* sendSync Sends SYNC and reads the number of bytes in the payload. Used both by
 * slaveMode() and getRDB().
 * returns 0 in case an EOF marker is used. */
func sendSync(c *RedisGo, outEof [RdbEofMarkSize]byte) uint64 {
	/* To start we need to send the SYNC command and return the payload.
	 * The hiredis client lib does not understand this part of the protocol
	 * and we don't want to mess with its buffers, so everything is performed
	 * using direct low-level I/O. */
	var buf = [4096]byte{}
	var p byte
	var err error
	/* Send the SYNC command. */
	err = c.Conn.Send("SYNC\r\n")
	if err != nil {
		clusterManagerLogFatalf("Error writing to master.")
	}
	/* Read $<payload>\r\n, making sure to read just up to "\n" */
	for {
		print(p)
		fmt.Println(buf)
		break
	}
	return 0
}
func clusterManagerGetNodeRDBFilename(node *ClusterManagerNode) string {
	var fileName string
	fileName = config.backupDir
	if !filepath.IsAbs(fileName) {
		fileName = filepath.Dir(fileName)
	}
	fileName = filepath.Join(fileName, fmt.Sprintf("redis-node-%s-%d-%s.rdb", node.Ip, node.Port, node.Name))
	return fileName
}
