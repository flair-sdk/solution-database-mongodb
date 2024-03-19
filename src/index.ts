import {
  EnricherEngine,
  FieldType,
  Schema,
  SolutionContext,
  SolutionDefinition,
} from 'flair-sdk'

export type Config = {
  schema: string | string[]
  connectionUri: string
  databaseName: string
  collectionsPrefix: string
}

const definition: SolutionDefinition<Config> = {
  prepareManifest: async (context, config, manifest) => {
    const mergedSchema = await loadSchema(context, config.schema)
    let streamingSql = `SET 'execution.runtime-mode' = 'STREAMING';`

    for (const entityType in mergedSchema) {
      try {
        const collectionName = `${config.collectionsPrefix || ''}${entityType}`

        const fieldsSql = Object.entries(mergedSchema[entityType])
          .map(
            ([fieldName, fieldType]) =>
              `\`${fieldName}\` ${getSqlType(fieldType)}`,
          )
          .join(', ')

        streamingSql += `
---
--- ${entityType}
---
CREATE TABLE source_${entityType} (
  ${fieldsSql}
  PRIMARY KEY (\`entityId\`) NOT ENFORCED;
) WITH (
  'connector' = 'stream',
  'mode' = 'cdc',
  'namespace' = '{{ namespace }}',
  'entity-type' = '${entityType}',
  'scan.startup.mode' = 'timestamp',
  'scan.startup.timestamp-millis' = '{{ chrono("2 hours ago") * 1000 }}'
);

CREATE TABLE sink_${entityType} (
  ${fieldsSql}
  PRIMARY KEY (\`entityId\`) NOT ENFORCED
) WITH (
  'connector' = 'mongodb',
  'uri' = '${config.connectionUri || '{{ secret("mongodb.uri") }}'}',
  'database' = 'indexer',
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
      `database/mongodb-${context.identifier}/streaming.sql`,
      streamingSql,
    )

    manifest.enrichers.push({
      id: `database-mongodb-${context.identifier}-streaming`,
      engine: EnricherEngine.Flink,
      size: 'small',
      inputSql: `database/mongodb-${context.identifier}/streaming.sql`,
    })

    return manifest
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
    case FieldType.String:
      return 'STRING'
    case FieldType.Integer:
      return 'BIGINT'
    case FieldType.Float:
      return 'DOUBLE'
    case FieldType.Boolean:
      return 'BOOLEAN'
    case FieldType.Object:
      return 'STRING'
    case FieldType.Array:
      return 'STRING'
    default:
      throw new Error(`Unsupported field type: ${fieldType}`)
  }
}
