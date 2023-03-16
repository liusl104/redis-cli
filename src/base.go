package redis_cli

import (
	"flag"
	"fmt"
	"github.com/fatih/color"
	"github.com/gomodule/redigo/redis"
	"github.com/siddontang/go-log/log"
	"os"
	"strings"
	"time"
)

const (
	RedisErr                                       = false
	RedisOk                                        = true
	ClusterManagerInvalidHostArg                   = "[ERR] Invalid arguments: you need to pass either a valid address (ie. 120.0.0.1:7000)"
	RedisCliKeepaliveInterval                      = 15 /* seconds */
	RedisCliTimeout                                = 5  // timeout
	RedidDbNum                                     = 0
	ClusterManagerOptGetFriends                    = 1 << 0
	ClusterManagerOptCold                          = 1 << 1
	ClusterManagerOptUpdate                        = 1 << 2
	ClusterManagerOptQuiet                         = 1 << 6
	ClusterManagerOptVerbose                       = 1 << 7
	ClusterManagerFlagMyself                       = 1 << 0
	ClusterManagerFlagSlave                        = 1 << 1
	ClusterManagerFlagFriend                       = 1 << 2
	ClusterManagerFlagNoAddr                       = 1 << 3
	ClusterManagerFlagDisconnect                   = 1 << 4
	ClusterManagerFlagFail                         = 1 << 5
	ClusterManagerCmdFlagFix                       = 1 << 0
	ClusterManagerCmdFlagSlave                     = 1 << 1
	ClusterManagerCmdFlagYes                       = 1 << 2
	ClusterManagerCmdFlagEmptymaster               = 1 << 4
	ClusterManagerCmdFlagSimulate                  = 1 << 5
	ClusterManagerCmdFlagReplace                   = 1 << 6
	ClusterManagerCmdFlagCopy                      = 1 << 7
	ClusterManagerCmdFlagCheckOwners               = 1 << 9
	ClusterManagerCmdFlagFixWithUnreachableMasters = 1 << 10
	ClusterManagerCmdFlagMastersOnly               = 1 << 11
	ClusterManagerCmdFlagSlavesOnly                = 1 << 12
	ClusterManagerCmdFlagFixFailOverMasters        = 1 << 13
	ClusterManagerSlots                            = 16384
	ClusterManagerMigrateTimeout                   = 60000
	ClusterManagerMigratePipeline                  = 10
	ClusterManagerRebalanceThreshold               = 2
	ClusterJoinCheckAfter                          = 20
	RdbEofMarkSize                                 = 40
	// UllongMax                                      = 18446744073709551615
)

var UllongMax uint64
var GitHash string

var crc16tab = [256]uint16{
	0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
	0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
	0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
	0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
	0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
	0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
	0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
	0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
	0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
	0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
	0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
	0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
	0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
	0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
	0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
	0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
	0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
	0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
	0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
	0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
	0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
	0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
	0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
	0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
	0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
	0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
	0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
	0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
	0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
	0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
	0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
	0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
}

var RedisPassword string
var RedisUser string

type Usage struct {
	Create                           string  `json:"create,omitempty"`
	ClusterReplicas                  int     `json:"cluster_replicas,omitempty"`
	Check                            string  `json:"check,omitempty"`
	ClusterSearchMultipleOwners      bool    `json:"cluster_search_multiple_owners,omitempty"`
	Info                             string  `json:"info,omitempty"`
	Fix                              string  `json:"fix,omitempty"`
	ClusterFixWithUnreachableMasters bool    `json:"cluster_fix_with_unreachable_masters,omitempty"`
	ClusterFixFailOverMasters        bool    `json:"cluster_fix_fail_over_masters,omitempty"`
	ReShard                          string  `json:"re_shard,omitempty"`
	ClusterFrom                      string  `json:"cluster_from,omitempty"`
	ClusterTo                        string  `json:"cluster_to,omitempty"`
	ClusterSlots                     int     `json:"cluster_slots,omitempty"`
	ClusterYes                       bool    `json:"cluster_yes,omitempty"`
	ClusterTimeout                   int     `json:"cluster_timeout,omitempty"`
	ClusterPipeline                  int     `json:"cluster_pipeline,omitempty"`
	ClusterReplace                   bool    `json:"cluster_replace,omitempty"`
	ReBalance                        string  `json:"re_balance,omitempty"`
	ClusterWeight                    string  `json:"cluster_weight,omitempty"`
	ClusterUseEmptyMasters           bool    `json:"cluster_use_empty_masters,omitempty"`
	ClusterSimulate                  bool    `json:"cluster_simulate,omitempty"`
	ClusterThreshold                 float64 `json:"cluster_threshold,omitempty"`
	AddNode                          string  `json:"add_node,omitempty"`
	ExistNode                        string  `json:"exist_node,omitempty"`
	CLusterSlave                     bool    `json:"cluster_slave,omitempty"`
	ClusterMasterID                  string  `json:"cluster_master_id,omitempty"`
	DelNode                          string  `json:"del_node,omitempty"`
	NodeID                           string  `json:"node_id,omitempty"`
	Call                             string  `json:"call,omitempty"`
	Command                          string  `json:"command,omitempty"`
	CLusterOnlyMasters               bool    `json:"c_luster_only_masters,omitempty"`
	CLusterOnlyReplicas              bool    `json:"c_luster_only_replicas,omitempty"`
	SetTimeout                       string  `json:"set_timeout,omitempty"`
	Milliseconds                     int     `json:"milliseconds,omitempty"`
	Import                           string  `json:"import,omitempty"`
	ClusterFromUser                  string  `json:"cluster_from_user,omitempty"`
	ClusterFromPass                  string  `json:"cluster_from_pass,omitempty"`
	ClusterFromAskPass               bool    `json:"cluster_from_ask_pass,omitempty"`
	ClusterCopy                      bool    `json:"cluster_copy,omitempty"`
	Backup                           string  `json:"backup,omitempty"`
	BackupDirectory                  string  `json:"backup_directory,omitempty"`
	Help                             bool    `json:"help,omitempty"`
	Version                          bool    `json:"version,omitempty"`
	User                             string  `json:"user,omitempty"`
	Password                         string  `json:"password,omitempty"`
	ShowLog                          bool    `json:"show_log,omitempty"`
	Verbose                          bool    `json:"verbose,omitempty"`
	// Tls                              bool    `json:"tls,omitempty"`
}

type RedisGo struct {
	Conn redis.Conn
}

type ClusterManagerNode struct {
	Context        *RedisGo
	Name           string
	Ip             string
	Port           int
	CurrentEpoch   int
	PingSent       int
	PingRecv       int
	Flags          int
	FlagsStr       []string /* Flags string representations */
	Replicate      string   /* Master ID if node is a slave */
	Dirty          int      /* Node has changes that can be flushed */
	Friends        []*ClusterManagerNode
	Migrating      map[int]string /* An array of sds where even strings are slots and odd strings are the destination node IDs. */
	Importing      map[int]string /* An array of sds where even strings are slots and odd strings are the source node IDs. */
	MigratingCount int            /* Length of the migrating array (migrating slots*2) */
	ImportingCount int            /* Length of the importing array (importing slots*2) */
	ReplicasCount  int
	Weight         float64 /* Weight used by rebalance */
	Balance        int     /* Used by rebalance */
	Slots          []int
	SlotsCount     int
}

// ClusterManager The Cluster Manager global structure
type ClusterManager struct {
	Nodes              []*ClusterManagerNode /* List of Nodes in the configuration. */
	UnreachableMasters int                   /* Masters we are not able to reach. */
	Errors             []error
}

type clusterManagerCommand struct {
	name        string
	argc        int
	argv        int
	flags       int
	replicas    int
	from        string
	to          string
	weight      string
	weightArgc  int
	masterId    string
	slots       int
	timeout     int
	pipeline    int
	threshold   float64
	backupDir   string
	fromUser    string
	fromPass    string
	fromAskPass int
	logLevel    int
	auth        string
	user        string
	tls         bool
	sslConfig   *cliSSLConfig
	verbose     bool
	simulate    bool
	rdbFileName string
	getRdbMode  bool
}

func (c *clusterManagerCommand) init(args *Usage) {
	if args.ShowLog {
		c.logLevel = 1
	}
	if args.User != "" {
		if args.Password == "" {
			clusterManagerLogFatalf("password cannot be empty")
		}
	}
	c.timeout = ClusterManagerMigrateTimeout
	c.pipeline = ClusterManagerMigratePipeline
	c.threshold = ClusterManagerRebalanceThreshold
	UllongMax = 18446744073709551615
	if args.ClusterFromAskPass {
		fmt.Printf("Please input import source node password: ")
		_, _ = fmt.Scanln(&c.fromPass)
	}
	c.verbose = args.Verbose

	if args.CLusterOnlyMasters {
		c.flags |= ClusterManagerCmdFlagMastersOnly
	}
	if args.CLusterOnlyReplicas {
		c.flags |= ClusterManagerCmdFlagSlavesOnly
	}
	if args.ClusterYes {
		c.flags |= ClusterManagerCmdFlagYes
	}
	if args.ClusterSimulate {
		c.flags |= ClusterManagerCmdFlagSimulate
	}
	if args.ClusterReplace {
		c.flags |= ClusterManagerCmdFlagReplace
	}
	if args.ClusterCopy {
		c.flags |= ClusterManagerCmdFlagCopy
	}
	if args.CLusterSlave {
		c.flags |= ClusterManagerCmdFlagSlave
	}
	if args.ClusterUseEmptyMasters {
		c.flags |= ClusterManagerCmdFlagEmptymaster
	}
	if args.ClusterSearchMultipleOwners {
		c.flags |= ClusterManagerCmdFlagCheckOwners
	}
	if args.ClusterFixWithUnreachableMasters {
		c.flags |= ClusterManagerCmdFlagFixWithUnreachableMasters
	}
	if args.ClusterFixFailOverMasters {
		c.flags |= ClusterManagerCmdFlagFixFailOverMasters
	}
	if args.ClusterMasterID != "" {
		c.masterId = args.ClusterMasterID
	}
	config.slots = args.ClusterSlots
	if args.ClusterTimeout != 0 {
		config.timeout = args.ClusterTimeout
	} else {
		config.timeout = ClusterManagerMigrateTimeout
	}
	config.pipeline = args.ClusterPipeline
	// config.threshold = args.ClusterThreshold
	config.masterId = args.ClusterMasterID
	config.replicas = args.ClusterReplicas
	config.from = args.ClusterFrom
	config.to = args.ClusterTo
	config.fromUser = args.ClusterFromUser
	config.fromPass = args.ClusterFromPass
	config.fromAskPass = 1
	config.auth = args.Password
	config.user = args.User
	if args.Version {
		var version string
		version = cliVersion()
		fmt.Printf("redis-cli for redis-6.2.7 (%s)\n", version)
		os.Exit(0)
	}
}

func cliVersion() string {
	return fmt.Sprintf("git:%s", GitHash)
}

var config *clusterManagerCommand
var clusterManager *ClusterManager

func showHelpInfo() {
	info := `Cluster Manager Commands:
  --create         host1:port1 ... hostN:portN
                   --cluster-replicas <arg>
  --check          host:port
                   --cluster-search-multiple-owners
  --info           host:port
  --fix            host:port
                   --cluster-search-multiple-owners
                   --cluster-fix-with-unreachable-masters
                   --cluster-fix-fail-over-masters
  --reshard        host:port
                   --cluster-from <arg>
                   --cluster-to <arg>
                   --cluster-slots <arg>
                   --cluster-yes
                   --cluster-timeout <arg>
                   --cluster-pipeline <arg>
                   --cluster-replace
  --rebalance      host:port
                   --cluster-weight <node1=w1...nodeN=wN>
                   --cluster-use-empty-masters
                   --cluster-timeout <arg>
                   --cluster-simulate
                   --cluster-pipeline <arg>
                   --cluster-threshold <arg>
                   --cluster-replace
  --add-node       new_host:new_port 
                   --exist-node existing_host:existing_port
                   --cluster-slave
                   --cluster-master-id <arg>
  --del-node       host:port 
                   --node-id <arg>
  --call           host:port 
                   --command arg arg .. arg
                   --cluster-only-masters
                   --cluster-only-replicas
  --set-timeout    host:port 
                   --milliseconds <arg>
  --import         host:port
                   --cluster-from <arg>
                   --cluster-from-user <arg>
                   --cluster-from-pass <arg>
                   --cluster-from-askpass
                   --cluster-copy
                   --cluster-replace
  --backup         host:port
                   --backup-directory <arg>
  --help   

For check, fix, reshard, del-node, set-timeout you can specify the host and port of any working node in the cluster.

Cluster Manager Options:
  --cluster-yes      Automatic yes to cluster commands prompts
  --cluster-log      Display messages as log output
  --verbose          Verbose mode.
  -a <password>      Password to use when connecting to the server.
                     You can also use the REDISCLI_AUTH environment
                     variable to pass this password more safely
                     (if both are used, this argument takes precedence).
  --user <username>  Used to send ACL style 'AUTH username pass'. Needs -a.
  --pass <password>  Alias of -a for consistency with the new --user option.

`
	fmt.Printf(info)
	os.Exit(0)
}

func intToBool(n int) bool {
	// 如果是0就是false 不等于0就是true
	if n == 0 {
		return false
	}
	return true
}
func diffIntToBool(n int) bool {
	// 如果是0就是false 不等于0就是true
	if n == 0 {
		return true
	}
	return false
}

func (r *RedisGo) Do(commandName string, args ...interface{}) (string, error) {
	var reply string
	var err error
	reply, err = redis.String(r.Conn.Do(commandName, args...))
	if err != nil {
		return "", err
	}
	return reply, nil
}

func (r *RedisGo) Close() {
	_ = r.Conn.Close()
}

type cliSSLConfig struct {
	/* Requested SNI, or NULL */
	sni string
	/* CA Certificate file, or NULL */
	cacert string
	/* Directory where trusted CA certificates are stored, or NULL */
	cacertdir string
	/* Skip server certificate verification. */
	skipCertVerify int
	/* Client certificate to authenticate with, or NULL */
	cert string
	/* Private key file to authenticate with, or NULL */
	key string
	/* Prefered cipher list, or NULL (applies only to <= TLSv1.2) */
	ciphers string
	/* Prefered ciphersuites list, or NULL (applies only to TLSv1.3) */
	ciphersuites string
}

func newRedisConn(network string, host string, port int, user string, password string) (*RedisGo, error) {
	conn, err := redis.Dial(network, fmt.Sprintf("%s:%d", host, port),
		redis.DialDatabase(RedidDbNum),
		redis.DialUsername(user),
		redis.DialPassword(password),
		redis.DialConnectTimeout(time.Duration(RedisCliTimeout)*time.Second),
		/* Set aggressive KEEP_ALIVE socket option in the Redis context socket
		 * in order to prevent timeouts caused by the execution of long
		 * commands. At the same time this improves the detection of real
		 * errors. */
		/*redis.DialUseTLS(config.tls),
		redis.DialTLSConfig(&tls.Config{
			Rand:                        nil,
			Time:                        nil,
			Certificates:                nil,
			GetCertificate:              nil,
			GetClientCertificate:        nil,
			GetConfigForClient:          nil,
			VerifyPeerCertificate:       nil,
			VerifyConnection:            nil,
			RootCAs:                     nil,
			NextProtos:                  nil,
			ServerName:                  "",
			ClientAuth:                  0,
			ClientCAs:                   nil,
			InsecureSkipVerify:          false,
			CipherSuites:                nil,
			SessionTicketsDisabled:      false,
			ClientSessionCache:          nil,
			MinVersion:                  0,
			MaxVersion:                  0,
			CurvePreferences:            nil,
			DynamicRecordSizingDisabled: false,
			Renegotiation:               0,
			KeyLogWriter:                nil,
		}),*/
		redis.DialKeepAlive(time.Duration(RedisCliKeepaliveInterval)*time.Second),
	)
	if err != nil {
		return nil, err
	}
	_, err = conn.Do("ping")
	if err != nil {
		return nil, err
	}
	rg := &RedisGo{Conn: conn}
	return rg, nil
}

func clusterManagerCommands(Args []string) (managerCommand *Usage) {
	managerCommand = new(Usage)
	flag.Usage = func() {
		showHelpInfo()
	}
	flag.StringVar(&managerCommand.Create, "create", "", "host1:port1, ... ,hostN:portN")
	flag.IntVar(&managerCommand.ClusterReplicas, "cluster-replicas", 0, "<arg>")
	flag.StringVar(&managerCommand.Check, "check", "", "host:port")
	flag.BoolVar(&managerCommand.ClusterSearchMultipleOwners, "cluster-search-multiple-owners", false, "")
	flag.StringVar(&managerCommand.Info, "info", "", "host:port")
	flag.StringVar(&managerCommand.Fix, "fix", "", "host:port")
	flag.BoolVar(&managerCommand.ClusterFixWithUnreachableMasters, "cluster-fix-with-unreachable-masters", false, "")
	flag.BoolVar(&managerCommand.ClusterFixFailOverMasters, "cluster-fix-fail-over-masters", false, "")
	flag.StringVar(&managerCommand.ReShard, "reshard", "", "host:port")
	flag.StringVar(&managerCommand.ClusterFrom, "cluster-from", "", "<arg>")
	flag.StringVar(&managerCommand.ClusterTo, "cluster-to", "", "<arg>")
	flag.IntVar(&managerCommand.ClusterSlots, "cluster-slots", 0, "<arg>")
	flag.BoolVar(&managerCommand.ClusterYes, "cluster-yes", false, "")
	flag.IntVar(&managerCommand.ClusterTimeout, "cluster-timeout", 0, "")
	flag.IntVar(&managerCommand.ClusterPipeline, "cluster-pipeline", 10, "<arg>")
	flag.BoolVar(&managerCommand.ClusterReplace, "cluster-replace", false, "")
	flag.StringVar(&managerCommand.ReBalance, "rebalance", "", "host:port")
	flag.StringVar(&managerCommand.ClusterWeight, "cluster-weight", "", "<node1=w1...nodeN=wN>")
	flag.BoolVar(&managerCommand.ClusterUseEmptyMasters, "cluster-use-empty-masters", false, "")
	flag.Float64Var(&managerCommand.ClusterThreshold, "cluster-threshold", 0.00, "<arg>")
	flag.BoolVar(&managerCommand.ClusterSimulate, "cluster-simulate", false, "")
	flag.StringVar(&managerCommand.AddNode, "add-node", "", "host:port")
	flag.StringVar(&managerCommand.ExistNode, "exist-node", "", "host:port")
	flag.BoolVar(&managerCommand.CLusterSlave, "cluster-slave", false, "")
	flag.StringVar(&managerCommand.ClusterMasterID, "cluster-master-id", "", "<arg>")
	flag.StringVar(&managerCommand.DelNode, "del-node", "", "host:port")
	flag.StringVar(&managerCommand.NodeID, "node-id", "", "<arg>")
	flag.StringVar(&managerCommand.Call, "call", "", "host:port")
	flag.StringVar(&managerCommand.Command, "command", "", "arg arg .. arg")
	flag.BoolVar(&managerCommand.CLusterOnlyMasters, "cluster-only-masters", false, "")
	flag.BoolVar(&managerCommand.CLusterOnlyReplicas, "cluster-only-replicas", false, "")
	flag.StringVar(&managerCommand.SetTimeout, "set-timeout", "", "host:port")
	flag.IntVar(&managerCommand.Milliseconds, "milliseconds", 0, "<arg>")
	flag.StringVar(&managerCommand.Import, "import", "", "host:port")
	flag.StringVar(&managerCommand.ClusterFromUser, "cluster-from-user", "", "<arg>")
	flag.StringVar(&managerCommand.ClusterFromPass, "cluster-from-pass", "", "<arg>")
	flag.BoolVar(&managerCommand.ClusterFromAskPass, "cluster-from-askpass", false, "")
	flag.BoolVar(&managerCommand.ClusterCopy, "cluster-copy", false, "")
	flag.StringVar(&managerCommand.Backup, "backup", "", "host:port")
	flag.StringVar(&managerCommand.BackupDirectory, "backup-directory", ".", "<arg>")
	flag.BoolVar(&managerCommand.Help, "help", false, "")
	flag.BoolVar(&managerCommand.Version, "version", false, "")
	flag.StringVar(&managerCommand.User, "user", "", "Used to send ACL style 'AUTH username pass'. Needs -a.")
	flag.StringVar(&managerCommand.User, "u", "", "Used to send ACL style 'AUTH username pass'. Needs -a.")
	flag.StringVar(&managerCommand.Password, "a", "", "Password to use when connecting to the server.")
	flag.StringVar(&managerCommand.Password, "pass", "", "Password to use when connecting to the server.")
	flag.BoolVar(&managerCommand.ShowLog, "cluster-log", false, "Display messages as log output")
	flag.BoolVar(&managerCommand.Verbose, "verbose", false, "Verbose mode.")
	// flag.BoolVar(&managerCommand.tls, "tls", false, "open tls connect")
	flag.Parse()
	if managerCommand.Help || len(Args) == 1 {
		showHelpInfo()
	}
	RedisPassword = managerCommand.Password
	RedisUser = managerCommand.User
	clusterManager = new(ClusterManager)
	return
}

func ParseOptions(args []string) {
	cm := clusterManagerCommands(args)
	config = new(clusterManagerCommand)
	config.init(cm)
	/* Cluster Manager commands. */
	switch {
	case cm.Info != "":
		clusterManagerCommandInfo(cm)
	case cm.Check != "":
		clusterManagerCommandCheck(cm)
	case cm.Fix != "":
		clusterManagerCommandFix(cm)
	case cm.Create != "":
		if len(strings.Split(cm.Create, ",")) < 3 {
			clusterManagerLogErr("[ERR] Wrong number of arguments for specified --cluster sub command")
			return
		}
		clusterManagerCommandCreate(cm)
	case cm.Call != "":
		clusterManagerCommandCall(cm)
	case cm.ReShard != "":
		clusterManagerCommandReshard(cm)
	case cm.ReBalance != "":
		if cm.ClusterWeight != "" {
			for i, s := range cm.ClusterWeight {
				if i+1 >= len(cm.ClusterWeight) {
					clusterManagerLogErr("[ERR] Wrong number of arguments for specified --cluster sub command")
					return
				}
				if s == '=' {
					break
				}

			}
		}

		clusterManagerCommandRebalance(cm)
	case cm.Backup != "":
		clusterManagerCommandBackup(cm)
	case cm.Import != "":
		clusterManagerCommandImport(cm)
	case cm.SetTimeout != "":
		clusterManagerCommandSetTimeout(cm)
	case cm.DelNode != "":
		if cm.NodeID == "" {
			clusterManagerLogFatalf("Unknown --cluster subcommand")
		}
		clusterManagerCommandDeleteNode(cm)
	case cm.AddNode != "":

		clusterManagerCommandAddNode(cm)
	default:
		showHelpInfo()
	}
	defer func() {
		if clusterManager.Nodes != nil {
			for _, node := range clusterManager.Nodes {
				_ = node.Context.Conn.Close()
			}
		}
	}()
}

// clusterManagerLogInfof 1 debug 2 info 3 war 4 error 5 fatalf
func clusterManagerLogInfof(msg string, args ...interface{}) {
	if !intToBool(config.logLevel) {
		fmt.Printf(msg, args...)
	} else {
		log.Infof(msg, args...)
	}
}

// clusterManagerLogInfo 1 debug 2 info 3 war 4 error 5 fatalf
func clusterManagerLogInfo(msg string, args ...interface{}) {
	if !intToBool(config.logLevel) {
		fmt.Printf(msg, args...)
		fmt.Printf("\n")
	} else {
		log.Infof(msg, args...)
	}
}

// clusterManagerLogErr 1 debug 2 info 3 war 4 error 5 fatalf
func clusterManagerLogErr(msg string, args ...interface{}) {
	if !intToBool(config.logLevel) {
		/*fmt.Printf(msg, args...)
		fmt.Printf("\n")*/
		color.Red(msg, args...)
	} else {
		log.Errorf(msg, args...)
	}
}

// clusterManagerLogWarn 1 debug 2 info 3 war 4 error 5 fatalf
func clusterManagerLogWarn(msg string, args ...interface{}) {
	if !intToBool(config.logLevel) {
		color.Yellow(msg, args...)
		/*fmt.Printf(msg, args...)
		fmt.Printf("\n")*/
	} else {
		log.Warnf(msg, args...)
	}
}

// clusterManagerLogOk 1 debug 2 info 3 war 4 error 5 fatalf
func clusterManagerLogOk(msg string, args ...interface{}) {
	if !intToBool(config.logLevel) {
		/*fmt.Printf(msg, args...)
		fmt.Printf("\n")*/
		color.Green(msg, args...)
	} else {
		log.Debugf(msg, args...)
	}
}

// clusterManagerLogFatalf 1 debug 2 info 3 war 4 error 5 fatalf
func clusterManagerLogFatalf(msg string, args ...interface{}) {
	if !intToBool(config.logLevel) {
		/*fmt.Printf(msg, args...)
		fmt.Printf("\n")*/
		color.Red(msg, args...)
		os.Exit(1)
	} else {
		log.Fatalf(msg, args...)
	}
}
