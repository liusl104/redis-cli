package redis_cli

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	okReply   = "OK"
	pongReply = "PONG"
)

type redisReplication struct {
	node        *ClusterManagerNode
	netType     string
	offset      int64
	masterRunId string
	rdbFileName string
	mu          sync.RWMutex
	pending     int
	err         error
	conn        net.Conn
	// Read
	readTimeout time.Duration
	br          *bufio.Reader
	// Write
	writeTimeout time.Duration
	bw           *bufio.Writer
	// Scratch space for formatting argument length.
	// '*' or '$', length, "\r\n"
	lenScratch [32]byte
	// Scratch space for formatting integers and floats.
	numScratch [40]byte
	replicator Replicator
	rdbBytes   int64
}

// Argument is the interface implemented by an object which wants to control how
// the object is converted to Redis bulk strings.
type Argument interface {
	// RedisArg returns a value to be encoded as a bulk string per the
	// conversions listed in the section 'Executing Commands'.
	// Implementations should typically return a []byte or string.
	RedisArg() interface{}
}

type Error string

type protocolError string

func (pe protocolError) Error() string {
	return fmt.Sprintf("redigo: %s (possible server error or unsupported concurrent read by application)", string(pe))
}

type Replicator interface {
	ProcessRdb(br io.Reader, rdbBytes int64) error

	ProcessMasterRepl(repl string) error
}

func (r *redisReplication) setOffset(offset int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.offset += offset
}
func (r *redisReplication) fatal(err error) error {
	r.mu.Lock()
	if r.err == nil {
		r.err = err
		// Close connection to force errors on subsequent calls and to unblock
		// other reader or writer.
		r.conn.Close()
	}
	r.mu.Unlock()
	return err
}
func (r *redisReplication) writeLen(prefix byte, n int) error {
	r.lenScratch[len(r.lenScratch)-1] = '\n'
	r.lenScratch[len(r.lenScratch)-2] = '\r'
	i := len(r.lenScratch) - 3
	for {
		r.lenScratch[i] = byte('0' + n%10)
		i -= 1
		n = n / 10
		if n == 0 {
			break
		}
	}
	r.lenScratch[i] = prefix
	_, err := r.bw.Write(r.lenScratch[i:])
	return err
}
func (r *redisReplication) writeString(s string) error {
	r.writeLen('$', len(s))
	r.bw.WriteString(s)
	_, err := r.bw.WriteString("\r\n")
	return err
}

func (r *redisReplication) writeBytes(p []byte) error {
	r.writeLen('$', len(p))
	r.bw.Write(p)
	_, err := r.bw.WriteString("\r\n")
	return err
}

func (r *redisReplication) writeInt64(n int64) error {
	return r.writeBytes(strconv.AppendInt(r.numScratch[:0], n, 10))
}

func (r *redisReplication) writeFloat64(n float64) error {
	return r.writeBytes(strconv.AppendFloat(r.numScratch[:0], n, 'g', -1, 64))
}

func (r *redisReplication) writeArg(arg interface{}, argumentTypeOK bool) (err error) {
	switch arg := arg.(type) {
	case string:
		return r.writeString(arg)
	case []byte:
		return r.writeBytes(arg)
	case int:
		return r.writeInt64(int64(arg))
	case int64:
		return r.writeInt64(arg)
	case float64:
		return r.writeFloat64(arg)
	case bool:
		if arg {
			return r.writeString("1")
		} else {
			return r.writeString("0")
		}
	case nil:
		return r.writeString("")
	case Argument:
		if argumentTypeOK {
			return r.writeArg(arg.RedisArg(), false)
		}
		// See comment in default clause below.
		var buf bytes.Buffer
		fmt.Fprint(&buf, arg)
		return r.writeBytes(buf.Bytes())
	default:
		// This default clause is intended to handle builtin numeric types.
		// The function should return an error for other types, but this is not
		// done for compatibility with previous versions of the package.
		var buf bytes.Buffer
		fmt.Fprint(&buf, arg)
		return r.writeBytes(buf.Bytes())
	}
}

func (r *redisReplication) writeCommand(cmd string, args []interface{}) error {
	r.writeLen('*', 1+len(args))
	if err := r.writeString(cmd); err != nil {
		return err
	}
	for _, arg := range args {
		if err := r.writeArg(arg, true); err != nil {
			return err
		}
	}
	return nil
}

// Send writes the command to the client's output buffer.
func (r *redisReplication) send(cmd string, args ...interface{}) error {
	r.mu.Lock()
	r.pending += 1
	r.mu.Unlock()
	if r.writeTimeout != 0 {
		r.conn.SetWriteDeadline(time.Now().Add(r.writeTimeout))
	}
	if err := r.writeCommand(cmd, args); err != nil {
		return r.fatal(err)
	}
	return nil
}

// Flush flushes the output buffer to the Redis server.
func (r *redisReplication) flush() error {
	if r.writeTimeout != 0 {
		r.conn.SetWriteDeadline(time.Now().Add(r.writeTimeout))
	}
	if err := r.bw.Flush(); err != nil {
		return r.fatal(err)
	}
	return nil
}

func (r *redisReplication) receive() (interface{}, error) {
	return r.receiveWithTimeout(r.readTimeout)
}

func (r *redisReplication) readReply() (interface{}, error) {
	var line []byte
	var err error
	line, err = r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, protocolError("short response line")
	}
	switch line[0] {
	case '+':
		switch {
		case len(line) == 3 && line[1] == 'O' && line[2] == 'K':
			// Avoid allocation for frequent "+OK" response.
			return okReply, nil
		case len(line) == 5 && line[1] == 'P' && line[2] == 'O' && line[3] == 'N' && line[4] == 'G':
			// Avoid allocation in PING command benchmarks :)
			return pongReply, nil
		default:
			return string(line[1:]), nil
		}
	case '-':
		return Error(string(line[1:])), nil
	case ':':
		return parseInt(line[1:])
	case '$':
		var n int
		n, err = parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		p := make([]byte, n)
		_, err = io.ReadFull(r.br, p)
		if err != nil {
			return nil, err
		}
		line, err = r.readLine()
		if err != nil {
			return nil, err
		} else if len(line) != 0 {
			return nil, protocolError("bad bulk string format")
		}
		return p, nil
	case '*':
		var n int
		n, err = parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		b := make([]interface{}, n)
		for i := range b {
			b[i], err = r.readReply()
			if err != nil {
				return nil, err
			}
		}
		return n, nil
	}
	return nil, protocolError("unexpected response line")
}

func (r *redisReplication) readLine() ([]byte, error) {
	p, err := r.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, protocolError("long response line")
	}
	if err != nil {
		return nil, err
	}
	i := len(p) - 2
	if i < 0 || p[i] != '\r' {
		return nil, protocolError("bad response line terminator")
	}
	return p[:i], nil
}

func (r *redisReplication) receiveWithTimeout(timeout time.Duration) (reply interface{}, err error) {
	var deadline time.Time
	if timeout != 0 {
		deadline = time.Now().Add(timeout)
	}
	err = r.conn.SetReadDeadline(deadline)
	if err != nil {
		return nil, err
	}

	if reply, err = r.readReply(); err != nil {
		return nil, r.fatal(err)
	}
	// When using pub/sub, the number of receives can be greater than the
	// number of sends. To enable normal use of the connection after
	// unsubscribing from all channels, we do not decrement pending to a
	// negative value.
	//
	// The pending field is decremented after the reply is read to handle the
	// case where Receive is called before Send.
	r.mu.Lock()
	if r.pending > 0 {
		r.pending -= 1
	}
	r.mu.Unlock()
	/*	if err, ok := reply.(Error); ok {
		return
	}*/
	return
}

func (r *redisReplication) reConnect() error {
	addr := fmt.Sprintf("%s:%d", r.node.Ip, r.node.Port)
	dial, err := net.Dial(r.netType, addr)
	if err != nil {
		return err
	}
	r.conn = dial
	r.bw = bufio.NewWriter(r.conn)
	r.br = bufio.NewReader(r.conn)
	if RedisUser != "" {
		err = r.send("AUTH2", RedisUser, RedisPassword)
		if err != nil {
			return err
		}
	} else {
		err = r.send("AUTH", RedisPassword)
		if err != nil {
			return err
		}
	}
	err = r.flush()
	if err != nil {
		return err
	}
	var reply interface{}
	reply, err = r.readReply()
	if err != nil {
		return err
	}
	if reply.(string) != "OK" {
		return err
	}
	return nil
}

// parseLen parses bulk string and array lengths.
func parseLen(p []byte) (int, error) {
	if len(p) == 0 {
		return -1, protocolError("malformed length")
	}

	if p[0] == '-' && len(p) == 2 && p[1] == '1' {
		// handle $-1 and $-1 null replies.
		return -1, nil
	}

	var n int
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return -1, protocolError("illegal bytes in length")
		}
		n += int(b - '0')
	}

	return n, nil
}

// parseInt parses an integer reply.
func parseInt(p []byte) (interface{}, error) {
	if len(p) == 0 {
		return 0, protocolError("malformed integer")
	}

	var negate bool
	if p[0] == '-' {
		negate = true
		p = p[1:]
		if len(p) == 0 {
			return 0, protocolError("malformed integer")
		}
	}

	var n int64
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return 0, protocolError("illegal bytes in length")
		}
		n += int64(b - '0')
	}

	if negate {
		n = -n
	}
	return n, nil
}

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
		getRDB(node)
	}
	json += "\n]"

	/* TODO: check if backup_dir is a valid directory. */
	var jsonPath []byte
	jsonPath = []byte(config.backupDir)
	if jsonPath[len(jsonPath)-1] != '/' {
		jsonPath = append(jsonPath, '/')
	}
	var jsonFilePath string
	jsonFilePath = string(jsonPath) + "nodes.json"
	clusterManagerLogInfo("Saving cluster configuration to: %s", jsonFilePath)
	var out *os.File
	out, err = os.OpenFile(jsonFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
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

func (r *redisReplication) ProcessRdb(br io.Reader, n int64) (err error) {
	var file *os.File
	file, err = os.OpenFile(r.rdbFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.CopyN(file, br, n)

	return err
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
	var err error
	if node != nil {
		if node.Context == nil {
			return
		}
		fileName = clusterManagerGetNodeRDBFilename(node)
	} else {
		fileName = config.rdbFileName
	}
	repl := &redisReplication{
		node:         node,
		rdbFileName:  fileName,
		offset:       -1,
		masterRunId:  "?",
		netType:      "tcp",
		readTimeout:  0,
		writeTimeout: 0,
	}

	err = sendSync(repl)
	if err != nil {
		clusterManagerLogFatalf("Fail to fsync '%s': %s", fileName, err.Error())
	}
}

/* sendSync Sends SYNC and reads the number of bytes in the payload. Used both by
 * slaveMode() and getRDB().
 * returns 0 in case an EOF marker is used. */
func sendSync(c *redisReplication) error {
	/* To start we need to send the SYNC command and return the payload.
	 * The hiredis client lib does not understand this part of the protocol
	 * and we don't want to mess with its buffers, so everything is performed
	 * using direct low-level I/O. */
	err := c.reConnect()
	if err != nil {
		return err
	}
	err = c.send("PSYNC", c.masterRunId, c.offset)
	if err != nil {
		return err
	}
	err = c.flush()
	var line []byte
	line, err = c.readLine()
	if line[0] == '+' {
		lineSplit := strings.Split(string(line[1:]), " ")
		if lineSplit[0] == "FULLRESYNC" {
			c.masterRunId = lineSplit[1]
			offset, _ := strconv.ParseInt(string(lineSplit[2]), 10, 64)
			c.setOffset(offset)
			for {
				line, err = c.readLine()
				if strings.HasPrefix(string(line), "$") {
					c.rdbBytes, err = strconv.ParseInt(string(line[1:]), 10, 64)

					break
				}
			}
			clusterManagerLogInfo("SYNC sent to master, writing %d bytes to '%s'", c.rdbBytes, c.rdbFileName)
			err = c.ProcessRdb(c.br, c.rdbBytes)
			if err != nil {
				return err
			}
			clusterManagerLogInfo("Transfer finished with success.")
			_ = c.conn.Close()
		}
	}

	return nil
}

func clusterManagerGetNodeRDBFilename(node *ClusterManagerNode) (fullFileName string) {
	var fileName []byte
	fileName = []byte(config.backupDir)
	if fileName[len(fileName)-1] != '/' {
		fileName = append(fileName, '/')
	}

	fullFileName = string(fileName) + fmt.Sprintf("redis-node-%s-%d-%s.rdb", node.Ip, node.Port, node.Name)
	return
}
