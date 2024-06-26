# MongoDB Database Integration

[![Flair](https://img.shields.io/badge/Powered%20by-Flair-ff69b4)](https://flair.dev)

This package provides components required to sync indexed data to a MongoDB instance in [Flair indexer](https://docs.flair.dev).

## Installation

1. Make sure you have created your indexing cluster as described in [Getting Started](https://docs.flair.dev/#getting-started).

2. Install the MongoDB solution package:
```bash
pnpm install @flair-sdk/solution-database-mongodb
```

3. Define your schemas based on your entities. For example, if you have a `Swap` entity, you can define a schema as follows:
```yaml
# ./src/schemas/Swap.yml
---
Swap:
  entityId: STRING
  entityUpdatedAt: INT64
  chainId: INT64
  poolAddress: STRING
  from: STRING
  to: STRING
  amount: INT256
  amountUSD: DOUBLE
  someObjectOrArray: STRING # all json stringified objects or arrays will be automatically stored as acttual nested documents/arrays in mongo
```

3. Add the MongoDB solution to your [manifest.yml.mustache](https://github.com/flair-sdk/starter-boilerplate/blob/main/manifest.yml.mustache) usually created from starter-boilerplate repository:
```yml
# ./manifest.yml.mustache
# ...
solutions:
  - source: '@flair-sdk/solution-database-mongodb'
    config:
      schema:
        - src/schemas/*.yml
      connectionUri: '{{{ mongoUri }}}'
      databaseName: flair
```
> The "mongoUri" here is a variable that is replaced by mustache locally. Refer to boilerplate repo for more details.

4. Add `mongoUri` to your [config.json](https://github.com/flair-sdk/starter-boilerplate/blob/main/config.prod.json) usually created from starter-boilerplate repository:
```js
// ./config.prod.json
{
  "cluster": "prod",
  "namespace": "my-project",
  // ...
  "mongoUri": "{{ secret(\"mongodb.uri\") }}"
}
```
> The "mongodb.uri" is an actual secret value set using [flair secret](https://docs.flair.dev/reference/database/mongodb) command, so that it is not exposed in the repository.

5. Deploy your cluster and check the status of real-time live syncing.
```sh
pnpm generate-and-deploy
pnpm flair logs -w -t component=enricher
```

6. (OPTIONAL) If there are any historical previously indexed data, you can sync them using:
```sh
pnpm flair script database-manual-full-sync
```
