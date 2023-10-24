# OversightML Tile Server

The OversightML Tile Server is a Python package allows you to quickly pass an image from S3 bucket to get metadata, image statistics, and set of tiles in real-time.

### Table of Contents
* [Getting Started](#getting-started)
  * [Package Layout](#package-layout)
  * [Prerequisites](prerequisites)
  * [Running Tile Server](#running-tile-server)
  * [Development Environment](#development-environment)
* [Support & Feedback](#support--feedback)
* [Security](#security)
* [License](#license)

## Getting Started

### Package Layout

* **/src**: This is the Python implementation of this application.
* **/test**: Unit tests have been implemented using [pytest](https://docs.pytest.org).
* **/scripts**: Utility scripts that are not part of the main application frequently used in development / testing.
* **/docs**: Contains Sphinx Doc configuration which is used to generate documentation for this package

### Prerequisites

First, ensure you have installed the following tools locally

- [docker](https://nodejs.org/en)
- [tox](https://tox.wiki/en/latest/installation.html)

### Running Tile Server

Build the Tile Server container

```shell
docker build . -t osml-tile-server:latest
```

To boot up the Tile Server, ensure you have the AWS credentials loaded!

```shell
./scripts/run-local-server.sh
```

In another terminal to invoke the rest server and return the viewpoint on a single image, run the following command:

```bash
curl -X 'POST' \
  'http://localhost:80/viewpoints/' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "bucket_name": "<S3 Bucket>",
  "object_key": "<Image Name>",
  "viewpoint_name": "test",
  "tile_size": 512,
  "range_adjustment": "NONE"
}'
```

Additionally, you can head over to FastAPI homepage and be able to execute various of API calls by visiting

```
http://0.0.0.0:80/docs or http://0.0.0.0:80/redoc
```

### Development Environment

Build the Tile Server container

```shell
docker build . -t osml-tile-server:latest
```

To build the container in a build/test mode and work inside it.

```shell
./scripts/run-interactive-gdal-container.sh
```

## Support & Feedback

To post feedback, submit feature ideas, or report bugs, please use the [Issues](https://github.com/aws-solutions-library-samples/osml-tile-server/issues) section of this GitHub repo.

If you are interested in contributing to OversightML Model Runner, see the [CONTRIBUTING](CONTRIBUTING.md) guide.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.
