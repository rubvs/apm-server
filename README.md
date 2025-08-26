## 1. No changes to container setup

- Setup
```shell
# Start with a clean docker environment
> docker rm -f $(docker ps -a -q) && docker rmi -f $(docker images -a -q)

# Ensure the test cache is clean
> go clean -testcache

> cd rumtest

# Run the rum system test
> go test -v -race -timeout=20m ./...
```

- Output (FAIL): Approval tests will fail.

## 2. Download database when container spin up

- Apply changes in [commit](https://github.com/rubvs/apm-server/commit/a85edcae4d0cd934be57e5c60459d9fd891d348c)
- Rerun steps outlined in (1)
- Output (SUCCESS): Approval tests will pass.

## 3 Download a new GeoIp database

- Replace the current filed in `/testing/docker/elasticsearch/ingest-geoip`.
- Output (SUCCESS): Approval tests will pass.
