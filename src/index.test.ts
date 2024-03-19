import { describe, it, expect, jest } from "@jest/globals";

import { EnricherEngine, FieldType, SolutionContext } from "flair-sdk";
import solutionDefinition from "./index.js";

describe("solution", () => {
  it("should generate streaming sql file", async () => {
    const context: jest.Mocked<SolutionContext> = {
      identifier: "default",
      readStringFile: jest.fn(),
      writeStringFile: jest.fn(),
      readYamlFile: jest.fn<any>(),
      writeYamlFile: jest.fn(),
      glob: jest.fn(),
    };

    context.glob.mockReturnValueOnce(["schema.yaml"]);
    context.readYamlFile.mockReturnValueOnce(
      Promise.resolve({
        Swap: {
          entityId: FieldType.String,
          amount: FieldType.Integer,
          amountUsd: FieldType.Float,
        },
      })
    );

    const updatedManifest = await solutionDefinition.prepareManifest(
      context,
      {
        schema: "schema.yaml",
        databaseName: "my_db",
        collectionsPrefix: "indexer_",
        connectionUri: '{{ secret("mongodb.uri") }}',
      },
      {
        manifest: "1.0.0",
        namespace: "my-test",
        cluster: { id: "dev" },
      }
    );

    expect(context.writeStringFile).toBeCalledWith(
      "database/mongodb-default/streaming.sql",
`SET 'execution.runtime-mode' = 'STREAMING';
---
--- Swap
---
CREATE TABLE source_Swap (
  \`entityId\` STRING, \`amount\` BIGINT, \`amountUsd\` DOUBLE
  PRIMARY KEY (\`entityId\`) NOT ENFORCED;
) WITH (
  'connector' = 'stream',
  'mode' = 'cdc',
  'namespace' = '{{ namespace }}',
  'entity-type' = 'Swap',
  'scan.startup.mode' = 'timestamp',
  'scan.startup.timestamp-millis' = '{{ chrono(\"2 hours ago\") * 1000 }}'
);

CREATE TABLE sink_Swap (
  \`entityId\` STRING, \`amount\` BIGINT, \`amountUsd\` DOUBLE
  PRIMARY KEY (\`entityId\`) NOT ENFORCED
) WITH (
  'connector' = 'mongodb',
  'uri' = '{{ secret(\"mongodb.uri\") }}',
  'database' = 'my_db',
  'collection' = 'indexer_Swap'
);

INSERT INTO sink_Swap SELECT * FROM source_Swap WHERE entityId IS NOT NULL;
`
    );

    expect(updatedManifest.enrichers?.length).toBe(1);
    expect(updatedManifest.enrichers?.[0]).toMatchObject({
      id: `database-mongodb-default-streaming`,
      engine: EnricherEngine.Flink,
      size: 'small',
      inputSql: `database/mongodb-default/streaming.sql`,
    });
  });
});
