import {
  EnricherEngine,
  FieldType,
  Schema,
  SolutionContext,
  SolutionDefinition,
  SolutionScriptFunction,
} from 'flair-sdk'

export type Config = {
  schema: string | string[]
  instance?: string
  connectionUri: string
  databaseName: string
  collectionsPrefix: string
}

const definition: SolutionDefinition<Config> = {
  prepareManifest: async (context, config, manifest) => {
    const mergedSchema = await loadSchema(context, config.schema)
    let streamingSql = `SET 'execution.runtime-mode' = 'STREAMING';`
    let batchSql = `SET 'execution.runtime-mode' = 'BATCH';`

    for (const entityType in mergedSchema) {
      try {
        const collectionName = `${config.collectionsPrefix || ''}${entityType}`

        if (!mergedSchema[entityType]?.entityId) {
          throw new Error(
            `entityId field is required, but missing for "${entityType}" in "${config.schema}"`,
          )
        }

        if (mergedSchema[entityType].entityId !== FieldType.STRING) {
          throw new Error(
            `entityId field must be of type STRING, but is of type "${mergedSchema[entityType].entityId}" for "${entityType}" in "${config.schema}"`,
          )
        }

        const fieldsSql = Object.entries(mergedSchema[entityType])
          .map(
            ([fieldName, fieldType]) =>
              `  \`${fieldName}\` ${getSqlType(fieldType)}`,
          )
          .join(',\n')

        streamingSql += `
---
--- ${entityType}
---
CREATE TABLE source_${entityType} (
${fieldsSql},
  PRIMARY KEY (\`entityId\`) NOT ENFORCED
) WITH (
  'connector' = 'stream',
  'mode' = 'cdc',
  'namespace' = '{{ namespace }}',
  'entity-type' = '${entityType}',
  'scan.startup.mode' = 'timestamp',
  'scan.startup.timestamp-millis' = '{{ chrono("2 hours ago") * 1000 }}'
);

CREATE TABLE sink_${entityType} (
${fieldsSql},
  PRIMARY KEY (\`entityId\`) NOT ENFORCED
) WITH (
  'connector' = 'mongodb',
  'uri' = '${config.connectionUri || '{{ secret("mongodb.uri") }}'}',
  'database' = '${config.databaseName}',
  'collection' = '${collectionName}'
);

INSERT INTO sink_${entityType} SELECT * FROM source_${entityType} WHERE entityId IS NOT NULL;
`

        const fields = Object.entries(mergedSchema[entityType])
        let timestampField = fields.find(
          ([fieldName, _fieldType]) => fieldName === 'blockTimestamp',
        )?.[0]
        if (!timestampField) {
          timestampField = fields.find(([fieldName, _fieldType]) =>
            fieldName?.toLowerCase().includes('timestamp'),
          )?.[0]
        }

        batchSql += `
---
--- ${entityType}
---
CREATE TABLE source_${entityType} (
${fieldsSql},
  PRIMARY KEY (\`entityId\`) NOT ENFORCED
) WITH (
  'connector' = 'database',
  'mode' = 'read',
  'namespace' = '{{ namespace }}',
  'entity-type' = '${entityType}'${
          timestampField
            ? `,
  'scan.partition.num' = '10',
  'scan.partition.column' = '${timestampField}',
  'scan.partition.lower-bound' = '{{ chrono(fromTimestamp | default("01-01-2020 00:00 UTC")) }}',
  'scan.partition.upper-bound' = '{{ chrono(toTimestamp | default("now")) }}'
  `
            : ''
        }
);

CREATE TABLE sink_${entityType} (
${fieldsSql},
  PRIMARY KEY (\`entityId\`) NOT ENFORCED
) WITH (
  'connector' = 'mongodb',
  'uri' = '${config.connectionUri || '{{ secret("mongodb.uri") }}'}',
  'database' = '${config.databaseName}',
  'collection' = '${collectionName}'
);

INSERT INTO sink_${entityType} SELECT * FROM source_${entityType} WHERE entityId IS NOT NULL;
`
      } catch (e: any) {
        throw new Error(
          `Failed to prepare manifest for entityType ${entityType}: ${
            e?.stack || e?.message || e?.toString()
          }`,
        )
      }
    }

    if (!manifest.enrichers?.length) {
      manifest.enrichers = []
    }

    context.writeStringFile(
      `database/mongodb-${config.instance || 'default'}/streaming.sql`,
      streamingSql,
    )
    context.writeStringFile(
      `database/mongodb-${config.instance || 'default'}/batch.sql`,
      batchSql,
    )

    manifest.enrichers.push(
      {
        id: `database-mongodb-${config.instance}-streaming`,
        engine: EnricherEngine.Flink,
        size: 'small',
        inputSql: `database/mongodb-${config.instance}/streaming.sql`,
      },
      {
        id: `database-mongodb-${config.instance}-batch`,
        engine: EnricherEngine.Flink,
        size: 'small',
        inputSql: `database/mongodb-${config.instance}/batch.sql`,
      },
    )

    // manifest.triggers.push({
    //   event: 'AfterBackfillSuccess',
    //   action: {
    //     method: 'triggerEnricher',
    //     payload: {
    //       id: `database-mongodb-${context.identifier}-batch`
    //     }
    //   }
    // });

    return manifest
  },
  registerScripts: (context, config): Record<string, SolutionScriptFunction> => {
    return {
      'database-manual-full-sync': async (_, options) => {
        context.runCommand('enricher:trigger', [
          `database-mongodb-${config.instance || 'default'}-batch`,
          ...(options?.fromTimestamp
            ? ['-p', `fromTimestamp='${options.fromTimestamp}'`]
            : []),
        ])
      },
    }
  },
}

export default definition

async function loadSchema(
  context: SolutionContext,
  schemas: string | string[],
): Promise<Schema> {
  const files = (Array.isArray(schemas) ? schemas : [schemas]).flatMap(
    (schema) => context.glob(schema),
  )

  if (!files.length) {
    throw new Error(
      `No schema files found for pattern(s) ${JSON.stringify(schemas)}`,
    )
  }

  const mergedSchema: Schema = {}

  for (const file of files) {
    try {
      const schema = await context.readYamlFile<Schema>(file)

      if (!schema || typeof schema !== 'object') {
        throw new Error(
          `Schema from ${file} must be an object defined in YAML format`,
        )
      }

      for (const [type, fields] of Object.entries(schema)) {
        if (!fields || typeof fields !== 'object') {
          throw new Error(
            `Fields for entityType ${type} in schema from ${file} must be an object`,
          )
        }

        if (mergedSchema[type]) {
          throw new Error(`Type ${type} is already defined in another schema`)
        }

        mergedSchema[type] = fields
      }
    } catch (e: any) {
      throw new Error(
        `Failed to load schema YAML from ${file}: ${
          e?.stack || e?.message || e?.toString()
        }`,
      )
    }
  }

  return mergedSchema
}

function getSqlType(fieldType: FieldType) {
  switch (fieldType) {
    case FieldType.STRING:
      return 'STRING'
    case FieldType.BIGINT:
      return 'BIGINT'
    case FieldType.DOUBLE:
      return 'DOUBLE'
    case FieldType.BOOLEAN:
      return 'BOOLEAN'
    case FieldType.ARRAY:
      return 'STRING'
    case FieldType.OBJECT:
      return 'STRING'
    default:
      throw new Error(`Unsupported field type: ${fieldType}`)
  }
}
