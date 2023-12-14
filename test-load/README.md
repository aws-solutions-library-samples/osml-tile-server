# Basic Load Testing with Locust

## Quick Start: Local Development Load Testing
Sample configuration files have been provided to run load tests from a local machine. The `test-load/locust_ts_user.py`
locustfile contains simulated users of the tile server resources.

1. Create a Conda environment containing the load testing tools. A sample environment has been provided in: `test-load/locust-environment.yaml`
2. Activate the Conda environment
3. Run Locust on your local machine and point at your running instance of the Tile Server (see -H option)
4. Assuming a default configuration the Locust web interface should be available at http://localhost:8089

```shell
cd test-load
conda env create -f locust-environment.yaml
conda activate osml-ts-locust-env
locust -f locust_ts_user.py -H http://localhost:8080/v1_0
```

## References:
- Locust: https://locust.io
