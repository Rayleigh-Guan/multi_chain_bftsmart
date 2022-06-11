# Throughput experiment command

## server:
Usage: ... ThroughputLatencyServer <processId> <measurement interval> <reply size> <state size> <context?>

```shell
sh ./runscripts/smartrun.sh bftsmart.demo.microbenchmarks.ThroughputLatencyServer 0 400 0 0 false > server0.out

sh ./runscripts/smartrun.sh bftsmart.demo.microbenchmarks.ThroughputLatencyServer 1 400 0 0 false > server1.out

sh ./runscripts/smartrun.sh bftsmart.demo.microbenchmarks.ThroughputLatencyServer 2 400 0 0 false > server2.out

sh ./runscripts/smartrun.sh bftsmart.demo.microbenchmarks.ThroughputLatencyServer 3 400 0 0 false > server3.out

sh ./runscripts/smartrun.sh bftsmart.demo.microbenchmarks.ThroughputLatencyServer 4 400 0 0 false > server4.out

sh ./runscripts/smartrun.sh bftsmart.demo.microbenchmarks.ThroughputLatencyServer 5 400 0 0 false > server5.out

sh ./runscripts/smartrun.sh bftsmart.demo.microbenchmarks.ThroughputLatencyServer 6 400 0 0 false > server6.out
```

## ThroughputLatencyClient:
```shell
sh ./runscripts/smartrun.sh bftsmart.demo.microbenchmarks.ThroughputLatencyClient 0 50 1000 1024 1 false true > client0.out
```

## AsyncLatencyClient
# Usage: java ...AsyncLatencyClient <initial client id> <number of clients> <number of operations> <request size> <interval (ms)> <read only?> <verbose?>
sh ./runscripts/smartrun.sh bftsmart.demo.microbenchmarks.AsyncLatencyClient 3 5 100 512 7 false true 

## test erasure code
sh ./runscripts/smartrun.sh bftsmart.demo.testErasureCode.TestErasureCodec 3 1 10240