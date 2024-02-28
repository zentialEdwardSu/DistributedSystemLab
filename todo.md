# todo
~~- [ ] rpc timeout to prevent goroutine-leaking~~
- [X] fast catchup
- [ ] becomeXXX func
- [X] batch start
- [X] apply 保序
- [ ] snapshoting
- [ ] (preVote follower->pre_candidate->candidate)[https://elsonlee.github.io/2019/02/27/etcd-prevote/]
- [ ] check quorum 保证Leader仍能访问Quorum number以上的follower 防止stale read
- [ ] (use progress to track replication)[https://github.com/etcd-io/etcd/blob/v3.3.10/raft/design.md#progress]
