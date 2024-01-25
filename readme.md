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
- 6.5840 labCode: git://g.csail.mit.edu/6.5840-golabs-2024 6.5840
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
  ... Passed --   0.8  3   21    5769    3
Test (2B): RPC byte count ...
  ... Passed --   2.0  3   76  121624   11
Test (2B): test progressive failure of followers ...
  ... Passed --   4.7  3  145   31543    3
Test (2B): test failure of leaders ...
  ... Passed --   5.3  3  208   48338    3
Test (2B): agreement after follower reconnects ...
  ... Passed --   4.2  3  125   32608    7
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.6  5  244   51180    3
Test (2B): concurrent Start()s ...
  ... Passed --   0.7  3   23    6649    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   6.2  3  218   56948    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  22.0  5 3952 4127397  102
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.1  3   58   16642   12
PASS
ok      6.5840/raft     51.616s
go test -run 2B  1.08s user 0.53s system 3% cpu 51.881 total
```
