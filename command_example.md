# Throughput experiment command

## server:
```shell
./runscripts/smartrun.sh bftsmart.demo.microbenchmarks.ThroughputLatencyServer 0 400 0 0 2 > server0.out

./runscripts/smartrun.sh bftsmart.demo.microbenchmarks.ThroughputLatencyServer 1 400 0 0 2 > server1.out

./runscripts/smartrun.sh bftsmart.demo.microbenchmarks.ThroughputLatencyServer 2 400 0 0 2 > server2.out

./runscripts/smartrun.sh bftsmart.demo.microbenchmarks.ThroughputLatencyServer 3 400 0 0 2 > server3.out
```

## ThroughputLatencyClient:
```shell
./runscripts/smartrun.sh bftsmart.demo.microbenchmarks.ThroughputLatencyClient 0 50 1000 1024 1 false true > client0.out
```

## AsyncLatencyClient
sh ./runscripts/smartrun.sh bftsmart.demo.microbenchmarks.AsyncLatencyClient 3 5 100 512 7 false true 

## test erasure code
sh ./runscripts/smartrun.sh bftsmart.demo.testErasureCode.TestErasureCodec 3 1 10240