# ika

## Run 

### Run with cache processor and expiration 12 seconds to the Pool processor with 1 node

```
./run-ika.sh cache://120000 pool://1=http://geth:8545
```

### Run with 4 Threads pool, gzip compression processor to the pool processor with 2 nodes

```
./run-ika.sh --threads=4 compress://gzip pool://http://geth-1:8545,http://geth-2:8545
```

### Run with Round-robin load-balancing processor

```
./run-ika.sh lb://tag1=http://geth-1:8545,tag2=http://geth-2:8545
```


### Run with RPC3 aware processor (`rpc3://` special cache which knows how to parse batches and use cache and exceptions)

```
./run-ika.sh rpc3://evm,10000,block  pool://tag1=http://geth-1:8545,tag2=http://geth-2:8545
```

### Run with AI processors:  `ai://` processor knows how to route to the model, `ai_tokens://` knows how to calculate tokens from response

```
./run-ika.sh rpc3://evm,10000,block  pool://tag1=http://geth-1:8545,tag2=http://geth-2:8545
```
