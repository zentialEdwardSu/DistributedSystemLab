# LAB 2: Raft

## What is Raft?

### Distributed Consensus?什么是分布式共识

一致性是数据副本的差异
共识是达成一致性的方法
分布式共识是分布式节点达成一致的方法

### Raft?

**Raft**是一种分布式共识算法。相比于**Paxos**,**Raft**具有可理解性

## Raft Process

**Raft**集群中的节点有三个状态
- Follower 
- Candidate
- Leader

### Leader Election
1. 所有节点在Follower状态下开始
2. 一个节点在未收到Leader节点的消息后会变为Candidate状态，给自己投票并让其余节点投票
3. 在得到大多数票(vote)后，Candidate节点会变为Leader节点

#### timeout control
- election timeout  Follower变为Cnadidate与Cnadidate发起重新投票的等待时间 150-300ms的随机数
> 未投票的节点在收到Cnadidate的投票要求后会投票并重置自己的election timeout，
> Candidate重投票一般发生于Candidate同票的情况，他们会在timeout重新发起投票
- heartbeat timeout Leader向Follower发送Append Entries的时间

### election term
从最新的Leaeder产生到一个Follower未在election timeout结束前收到来自Leader的heartbeat的时间段

在一个term里面只有可能有一个Leader

在有多个不同期的Leader时，term高的会取代term低的，此时集群会进行term的同步，同步时小于最高term的节点会变为Follower并回滚log entry

### LogReplication
1. Leader 接收数据，生成log entry
2. Leader将log entry与Follower节点同步
3. Leader确认大多数Follower节点都写入了log entry后，才将log entry提交，进行entry commit更新自身状态。
4. Leader告知Follower节点进行entry ommit


