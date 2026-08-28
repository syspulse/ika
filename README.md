# ika

<img src="doc/logo-1.png" style="width: 10%;">   

`ika` is a modular L-7 HTTP Proxy Gateway which allows to process different HTTP service with 
pluggable middleware pipline

Out of the box `ika` supports:

- Request/Response Cache
- Connection pools (Load-balance, Sticky)
- Retry 
- Throttling
- Rejections
- Destination Request enirchment (method, headers)

## Processors

`ika` is configured as a sequence of processors which forward Request and Response down the Session pipeline.
Nearly every functionality is implemented as a Proccessor.
Every processor has its own unique `type` and custom parameters

Session Pipeline is configued via `profile` as a list of processors in this pipeline.

For example, simple Load-balancer:

```
pool_1 {
  type="pool://"
  strategy="lb"
  destinations = [
    "host1=http://localhost:8300",
    "host2=http://localhost:8301"
  ]
}

http_1 {
  type="http://"
}

profiles {
  load_balancer_1 = {
    processors = "pool_1, http_1"
  }
}
```

## Custom Processors

`ika` has been specifically desined to support Custom processors:

1. `rpc3` - Web3 RPC API (Evm, Solana) with request batching 
2. `ai_router` - LLM API Router

For Custom processors configuration, refer to [conf/application-ika.conf](conf/application-ika.conf)


## Run 

### Run via Profile

Profile is configured in [conf/application.conf](conf/application.conf):

```
pool_1 {
  type="pool://"
  strategy="lb"
  destinations = [
    "host1=http://localhost:8300",
    "host2=http://localhost:8301"
  ]
}

retry_2 {
  type="retry://"
  maxRetries=3
  delay=1000
}

http_2 {
  type="http://"
  connectTimeout=1000
  responseTimeout=3000
}

profiles {    
  
  proxy3 = {
    processors = "pool_1, retry_2, http_2"
  }
  
}
```

Running with profile:

Proxy Profile:
```
./run-ika.sh --profile proxy3
```


### Run via pipeline

Run with cache processor and expiration 12 seconds to the Pool processor with 1 node
```
./run-ika.sh cache://120000 pool://1=http://geth:8545
```

Run with 4 Threads pool, gzip compression processor to the pool processor with 2 nodes

```
./run-ika.sh --threads=4 compress://gzip pool://http://geth-1:8545,http://geth-2:8545
```

Run with Round-robin load-balancing processor
```
./run-ika.sh lb://tag1=http://geth-1:8545,tag2=http://geth-2:8545
```


Run with RPC3 aware processor (`rpc3://` special cache which knows how to parse batches and use cache and exceptions)
```
./run-ika.sh rpc3://evm,10000,block  pool://tag1=http://geth-1:8545,tag2=http://geth-2:8545
```

Run with AI processors:  `ai://` processor knows how to route to the model, `ai_tokens://` knows how to calculate tokens from response

```
./run-ika.sh rpc3://evm,10000,block  pool://tag1=http://geth-1:8545,tag2=http://geth-2:8545
```

