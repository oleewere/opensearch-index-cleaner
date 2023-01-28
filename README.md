# Opensearch Index Cleaner

![build workflow](https://github.com/oleewere/opensearch-index-cleaner/actions/workflows/docker-build.yml/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Tool for cleaning up old (aiven) opensearch indices.

## Setup

Copy `.env.template` content to a `.env` file and fill the variables. (Variable descriptions can be found in the template file)

### Rules file

Indices will be cleaned up up daily based on rules files. A rule file contains a list of objects that contains the Aiven service name and a list of rules that defines how old indeces (by index name pattern) should be deleted (based on days). You can also define `date_pattern` that represents the date format in the end of the index name. 

Rules file example:

```yaml
- service: myservice
  rules:
  - index_pattern: "*-myindex-*"
    age_threshold: 2
    date_pattern: "%Y.%m.%d" # that is the default if not defined
```

Note: make sure your indices has a date suffix. (required)

### Notification

You can set `NOTIFICATION_WEBHOOK_URL` to a valid slack webhook URL. Additionally to that you can set summary content for the notification itself. That can be defined in the rules file (for every service).

```yaml
  summary_reports:
    - pattern: "*myindex-*"
      name: My indices
```

It will print the pre-cleanup size for the indices (those that matches on the `pattern`), the used reference will be the `name` field in the notification content.

## Usage

Once environment variables are set up correctly just run:

```bash
cargo run
```

Or build and run container:

```bash
docker build -t oleewere/opensearch-index-cleaner .
docker run --rm -v $(pwd)/.env:/app/.env oleewere/opensearch-index-cleaner 
```

## TODO

Support simple OpenSearch and ElasticSearch.
