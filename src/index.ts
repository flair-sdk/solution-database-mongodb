import { DatabaseSyncEnricherParameters } from './types.js'
import {
  AppError,
  EnricherEngine,
  EnricherSize,
  FieldType,
  Schema,
  SolutionContext,
  SolutionDefinition,
  SolutionScriptFunction,
} from 'flair-sdk'

export type EntityOption = {
  batchPartitions?: number
}

export type Config = {
  schema: string | string[]
  instance?: string
  connectionUri: string
  databaseName: string
  collectionsPrefix: string
  flink?: {
    logLevel?: string
    instanceSize?: EnricherSize
    entityOverrides?: Record<string, EntityOption>
    defaultBatchSlots?: number
  }
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
  'scan.partition.num' = '${
    config?.flink?.entityOverrides?.[entityType]?.batchPartitions || 20
  }',
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
        throw AppError.causedBy(e, {
          code: 'ManifestPreparationError',
          message: 'Failed to prepare manifest for user-defined entity',
          details: {
            entityType,
          },
        })
      }
    }

    if (!manifest.enrichers?.length) {
      manifest.enrichers = []
    }

    const instance = config.instance || 'default'

    context.writeStringFile(
      `database/mongodb-${instance}/streaming.sql`,
      streamingSql,
    )
    context.writeStringFile(`database/mongodb-${instance}/batch.sql`, batchSql)

    manifest.enrichers.push(
      {
        id: `database-mongodb-${instance}-streaming`,
        engine: EnricherEngine.Flink,
        size: config?.flink?.instanceSize || 'small',
        inputSql: `database/mongodb-${instance}/streaming.sql`,
      },
      {
        id: `database-mongodb-${instance}-batch`,
        engine: EnricherEngine.Flink,
        size: config?.flink?.instanceSize || 'small',
        inputSql: `database/mongodb-${instance}/batch.sql`,
      },
    )

    return manifest
  },
  registerScripts: (
    context,
    config,
  ): Record<string, SolutionScriptFunction> => {
    const instance = config.instance || 'default'
    return {
      'database-manual-full-sync': {
        run: async (params: DatabaseSyncEnricherParameters) => {
          await context.runCommand('enricher:trigger', [
            `database-mongodb-${instance}-batch`,
            ...(params?.fromTimestamp
              ? ['-p', `fromTimestamp='${params.fromTimestamp}'`]
              : []),
            ...(params?.toTimestamp
              ? ['-p', `toTimestamp='${params.toTimestamp}'`]
              : []),
            ...(params?.autoApprove ? ['--auto-approve'] : []),
            ...(config?.flink?.defaultBatchSlots
              ? ['-o', `slots=${config.flink.defaultBatchSlots}`]
              : []),
            ...(params?.logLevel
              ? ['-l', `${params.logLevel}`]
              : config?.flink?.logLevel
              ? ['-l', `${config.flink.logLevel}`]
              : []),
          ])
        },
      },
    }
  },
  registerHooks: async (context) => {
    return [
      {
        for: 'pre-deploy',
        id: 'infer-schema',
        title: 'infer schema (optional)',
        run: async (params?: { autoApprove?: boolean }) => {
          await context.runCommand('util:infer-schema', [
            ...(params?.autoApprove ? ['--auto-approve'] : []),
          ])
        },
      },
      {
        for: 'pre-deploy',
        id: 'deploy-streaming',
        title: 'configure real-time sync',
        run: async (params?: { autoApprove?: boolean }) => {
          await context.runCommand('deploy', [
            '--skip-hooks',
            '--do-not-exit',
            ...(params?.autoApprove ? ['--auto-approve'] : []),
          ])
        },
      },
      {
        for: 'pre-deploy',
        id: 'mongodb-full-sync',
        title: 'one-off historical sync for mongodb',
        run: async (params?: { autoApprove?: boolean; logLevel?: string }) => {
          await context.runCommand('script', [
            'database-manual-full-sync',
            JSON.stringify(params || {}),
          ])
        },
      },
    ]
  },
}

export default definition

async function loadSchema(
  context: SolutionContext<Config>,
  schemas: string | string[],
): Promise<Schema> {
  const arrayedSchemas = Array.isArray(schemas) ? schemas : [schemas]
  const files = arrayedSchemas.flatMap((schema) => context.glob(schema))

  if (!files.length) {
    console.warn(`No schema files found in: ${arrayedSchemas.join(' ')}`)
  }

  const mergedSchema: Schema = {}

  for (const file of files) {
    try {
      const schema = await context.readYamlFile<Schema>(file)

      if (!schema || typeof schema !== 'object') {
        throw new AppError({
          code: 'InvalidSchemaError',
          message: 'Schema must be an object defined in YAML format',
          details: {
            file,
          },
        })
      }

      for (const [type, fields] of Object.entries(schema)) {
        if (!fields || typeof fields !== 'object') {
          throw new AppError({
            code: 'InvalidSchemaError',
            message: 'Fields for entitiy schema must be an object',
            details: {
              entityType: type,
              file,
            },
          })
        }

        if (mergedSchema[type]) {
          throw new AppError({
            code: 'DuplicateSchemaError',
            message: 'Entity type is already defined in another schema',
            details: {
              entityType: type,
              file,
            },
          })
        }

        mergedSchema[type] = fields
      }
    } catch (e: any) {
      throw AppError.causedBy(e, {
        code: 'SchemaLoadError',
        message: 'Failed to load schema YAML',
        details: {
          file,
        },
      })
    }
  }

  return mergedSchema
}

function getSqlType(fieldType: FieldType) {
  switch (fieldType) {
    case FieldType.STRING:
      return 'STRING'
    case FieldType.INT256:
      return 'STRING'
    case FieldType.INT64:
      return 'BIGINT'
    case FieldType.FLOAT8:
      return 'DOUBLE'
    case FieldType.BOOLEAN:
      return 'BOOLEAN'
    case FieldType.ARRAY:
      return 'STRING'
    case FieldType.OBJECT:
      return 'STRING'
    default:
      throw new Error(
        `Unsupported field type: ${fieldType} select from: ${Object.values(
          FieldType,
        ).join(', ')}`,
      )
  }
}
