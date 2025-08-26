```shell
# Start with a clean docker environment
> docker rm -f $(docker ps -a -q) && docker rmi -f $(docker images -a -q)

# Ensure the test cache is clean
> go clean -testcache

> cd rumtest

# Run the rum system test
> go test -v -race -timeout=20m ./...
```
