# lab for MIT 6.5840

my implement for Lab of [MIT 6.5840](https://pdos.csail.mit.edu/6.824/)

## Lab status
- [X] Lab1(All test passed)
- [ ] Lab2
    - [X] Lab2A 
    - [X] Lab2B 
    - [ ] Lab2C (working on)
    - [ ] Lab2D
- [ ] Lab3
- [ ] Lab4

## Citation
- [6.5840 course page](https://pdos.csail.mit.edu/6.824/index.html)
- 6.5840 labCode: git://g.csail.mit.edu/6.5840-golabs-2023 6.5840
- [MapReduce: Simplified Data Processing on Large Clusters](https://research.google/pubs/mapreduce-simplified-data-processing-on-large-clusters/)
- [raft page](https://raft.github.io/)
- [In Search of an Understandable Consensus Algorithm (Extended Version)](https://raft.github.io/raft.pdf)
- [Hashicorp Raft](https://github.com/hashicorp/raft)
- [cmap](https://github.com/lrita/cmap)

## Test result

> **Notice**: 
test may failed(multiLeader/can't reach agreement) due to 
Failure to set ElectionTimeout and HeartBeatTimeout carefully

### 2A

```bash
Test (2A): initial election ...
  ... Passed --   3.1  3   86   23444    0
Test (2A): election after network failure ...
  ... Passed --   4.9  3  104   21636    0
Test (2A): multiple elections ...
  ... Passed --   5.5  7  449   97133    0
PASS
ok      6.5840/raft     13.419s
go test -run 2A  0.37s user 0.27s system 4% cpu 13.716 total

```
### 2B

```bash
Test (2B): basic agreement ...
  ... Passed --   0.4  3   19    5674    3
Test (2B): RPC byte count ...
  ... Passed --   0.9  3   68  131264   11
Test (2B): test progressive failure of followers ...
  ... Passed --   4.4  3  122   27212    3
Test (2B): test failure of leaders ...
  ... Passed --   4.5  3  138   32989    3
Test (2B): agreement after follower reconnects ...
  ... Passed --   5.0  3  170   48394    8
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.3  5  240   51695    4
Test (2B): concurrent Start()s ...
  ... Passed --   0.6  3   20    6184    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   3.6  3  123   32874    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  10.4  5 2184 2306499  102
Test (2B): RPC counts arent too high ...
  ... Passed --   2.1  3   72   22490   12
PASS
ok      6.5840/raft     35.238s
go test -run 2B -race  2.04s user 1.50s system 9% cpu 37.554 total
```

```bash
Test (2C): basic persistence ...
  ... Passed --   2.9  3   94   26695    6
Test (2C): more persistence ...
  ... Passed --  14.0  5  985  234116   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   1.2  3   43   11716    4
Test (2C): Figure 8 ...
  ... Passed --  29.5  5 1415  342308   69
Test (2C): unreliable agreement ...
  ... Passed --   3.5  5  956  722911  246
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  39.7  5 17535 63457933   83
Test (2C): churn ...
  ... Passed --  16.1  5 6052 17276044  527
Test (2C): unreliable churn ...
  ... Passed --  16.3  5 7539 26118515  509
PASS
ok      6.5840/raft     123.459s
go test -run 2C -race  48.03s user 3.50s system 41% cpu 2:03.81 total
```
