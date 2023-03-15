
# redis-cli

本项目是基于redis 6.2.7 源代码使用go改写，实现redis-cli --cluster 所有功能

## 安装与编译

使用 shell 安装 redis-cli

```bash
  git clone https://github.com/liusl104/redis-cli.git
  cd redis-cli
  make

```

## 帮助

```shell
cd redis-cli
./cmd/redis-cli --help
Cluster Manager Commands:
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

```


## 特性

- 支持跨平台 mac、linux、windows、arm



## 文档

[redis 文档](https://redis.io/docs)

[redis 命令](https://redis.io/commands/)

[redis 源码](https://github.com/redis/redis/tree/6.2)
## 技术栈

**开发版本:** go 1.19



## 运行测试

要运行测试，运行以下命令

```bash
$ ./cmd/redis-cli --check 127.0.0.1:6380 -a 123456
127.0.0.1:6382 (34e83fbf...) -> 32124 keys | 5462 slots | 1 slaves.
127.0.0.1:6384 (8e097072...) -> 32299 keys | 5461 slots | 1 slaves.
127.0.0.1:6380 (f11555fc...) -> 32051 keys | 5461 slots | 1 slaves.
[OK] 96474 keys in 3 masters.
5.89 keys per slot on average.
>>> Performing Cluster Check (using node 127.0.0.1:6382)
M: 34e83fbf5b269a587560f5996c6c5e8683cc0a5c 127.0.0.1:6382
   slots:[5461],[10923-16383] (5462 slots) master
   1 additional replica(s)
M: 8e097072264e082ec9331cc49366adbf3c5a28d3 127.0.0.1:6384
   slots:[5462-10922] (5461 slots) master
   1 additional replica(s)
M: f11555fc814fdfa1c8f33621ffc73bb2b1c6b12c 127.0.0.1:6380
   slots:[0-5460] (5461 slots) master
   1 additional replica(s)
S: 0665005dc626ba717efb5ba8f157ed7cf009691a 127.0.0.1:6385
   slots: (0 slots) slave
   replicates 34e83fbf5b269a587560f5996c6c5e8683cc0a5c
S: 4b0fa237a2d132ce21da2688fbf42fc71860c43d 127.0.0.1:6383
   slots: (0 slots) slave
   replicates f11555fc814fdfa1c8f33621ffc73bb2b1c6b12c
S: cdc018d943bf57bcf0d6389e2532a72752c6a8ff 127.0.0.1:6381
   slots: (0 slots) slave
   replicates 8e097072264e082ec9331cc49366adbf3c5a28d3
[OK] All Nodes agree about slots configuration.
>>> Check for open slots...
>>> Check slots coverage...
[OK] All 16384 slots covered.


```


## 作者

- [@liusl104](https://github.com/liusl104)


## 反馈

如果你有任何反馈，请联系我们：liusl104@gmail.com,也可以直接提交issues

