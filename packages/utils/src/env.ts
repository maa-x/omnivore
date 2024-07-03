/* eslint-disable @typescript-eslint/no-unsafe-return */
/* eslint-disable @typescript-eslint/naming-convention */
/* eslint-disable @typescript-eslint/no-explicit-any */
import * as dotenv from 'dotenv'
import os from 'os'

interface redisConfig {
  url?: string
  cert?: string
}

export interface BackendEnv {
  pg: {
    host: string
    port: number
    userName: string
    password: string
    dbName: string
    pool: {
      max: number
    }
    replication: boolean
    replica: {
      host: string
      port: number
      userName: string
      password: string
      dbName: string
    }
  }
  server: {
    jwtSecret: string
    ssoJwtSecret: string
    gateway_url: string
    apiEnv: string
    instanceId: string
    trustProxy: boolean
    internalApiUrl: string
  }
  client: {
    url: string,
    api_url: string
  }
  google: {
    auth: {
      iosClientId: string
      androidClientId: string
      clientId: string
      secret: string
    }
  }
  posthog: {
    apiKey: string
  }
  intercom: {
    token: string
    secretKey: string
  }
  sentry: {
    dsn: string
  }
  jaeger: {
    host: string
  }
  imageProxy: {
    url: string
    secretKey: string
  }
  twitter: {
    token: string
  }
  dev: {
    isLocal: boolean
  }
  queue: {
    location: string
    name: string
    contentFetchUrl: string
    contentFetchGCFUrl: string
    reminderTaskHandlerUrl: string
    integrationTaskHandlerUrl: string
    textToSpeechTaskHandlerUrl: string
    recommendationTaskHandlerUrl: string
    thumbnailTaskHandlerUrl: string
    integrationExporterUrl: string
    integrationImporterUrl: string
    importerMetricsUrl: string
  }
  fileUpload: {
    gcsUploadBucket: string
    gcsUploadSAKeyFilePath: string
    gcsUploadPrivateBucket: string
    dailyUploadLimit: number
  }
  sender: {
    message: string
    feedback: string
    general: string
  }
  sendgrid: {
    confirmationTemplateId: string
    reminderTemplateId: string
    resetPasswordTemplateId: string
    installationTemplateId: string
    verificationTemplateId: string
  }
  readwise: {
    apiUrl: string
  }
  gcp: {
    location: string
  }

  pocket: {
    consumerKey: string
  }
  subscription: {
    feed: {
      max: number
    }
  }
  redis: {
    mq: redisConfig
    cache: redisConfig
  }
  notion: {
    clientId: string
    clientSecret: string
    authUrl: string
  }
  score: {
    apiUrl: string
  }
}

const nullableEnvVars = [
  // All env vars added for now, until a better way to manage this per package is found
'API_ENV',
'API_PUBLIC_URL',
'CLIENT_URL',
'CONTENT_FETCH_GCF_URL',
'CONTENT_FETCH_URL',
'GAE_INSTANCE',
'GATEWAY_URL',
'GAUTH_ANDROID_CLIENT_ID',
'GAUTH_CLIENT_ID',
'GAUTH_IOS_CLIENT_ID',
'GAUTH_SECRET',
'GCP_LOCATION',
'GCS_UPLOAD_BUCKET',
'GCS_UPLOAD_DAILY_LIMIT',
'GCS_UPLOAD_PRIVATE_BUCKET',
'GCS_UPLOAD_SA_KEY_FILE_PATH',
'IMAGE_PROXY_SECRET',
'IMAGE_PROXY_URL',
'IMPORTER_METRICS_COLLECTOR_URL',
'INTEGRATION_EXPORTER_URL',
'INTEGRATION_IMPORTER_URL',
'INTEGRATION_TASK_HANDLER_URL',
'INTERCOM_SECRET_KEY',
'INTERCOM_TOKEN',
'INTERNAL_API_URL',
'JAEGER_HOST',
'JWT_SECRET',
'MQ_REDIS_CERT',
'MQ_REDIS_URL',
'NOTION_AUTH_URL',
'NOTION_CLIENT_ID',
'NOTION_CLIENT_SECRET',
'PG_DB',
'PG_HOST',
'PG_PASSWORD',
'PG_POOL_MAX',
'PG_PORT',
'PG_REPLICA_DB',
'PG_REPLICA_HOST',
'PG_REPLICA_PASSWORD',
'PG_REPLICA_PORT',
'PG_REPLICATION',
'PG_REPLICA_USER',
'PG_USER',
'POCKET_CONSUMER_KEY',
'POSTHOG_API_KEY',
'PUPPETEER_QUEUE_LOCATION',
'PUPPETEER_QUEUE_NAME',
'READWISE_API_URL',
'RECOMMENDATION_TASK_HANDLER_URL',
'REDIS_CERT',
'REDIS_URL',
'REMINDER_TASK_HANDLER_URL',
'SAMPLE_METRICS_LOCALLY',
'SCORE_API_URL',
'SENDER_FEEDBACK',
'SENDER_GENERAL',
'SENDER_MESSAGE',
'SENDGRID_CONFIRMATION_TEMPLATE_ID',
'SENDGRID_INSTALLATION_TEMPLATE_ID',
'SENDGRID_REMINDER_TEMPLATE_ID',
'SENDGRID_RESET_PASSWORD_TEMPLATE_ID',
'SENDGRID_VERIFICATION_TEMPLATE_ID',
'SENTRY_AUTH_TOKEN',
'SENTRY_DSN',
'SENTRY_ORG',
'SENTRY_PROJECT',
'SSO_JWT_SECRET',
'SUBSCRIPTION_FEED_MAX',
'TEXT_TO_SPEECH_TASK_HANDLER_URL',
'THUMBNAIL_TASK_HANDLER_URL',
'TRUST_PROXY',
'TWITTER_BEARER_TOKEN',
] // Allow some vars to be null/empty

const envParser =
  (env: { [key: string]: string | undefined }) =>
  (varName: string): string => {
    const value = env[varName]
    if (typeof value === 'string' && value) {
      return value
    } else if (nullableEnvVars.includes(varName)) {
      return ''
    }
    throw new Error(
      `Missing ${varName} with a non-empty value in process environment`
    )
  }

interface Dict<T> {
  [key: string]: T | undefined
}

export function getEnv(): BackendEnv {
  // Dotenv parses env file merging into proces.env which is then read into custom struct here.
  dotenv.config()

  /* If not in GAE and Prod/QA/Demo env (f.e. on localhost/dev env), allow following env vars to be null */
  if (process.env.API_ENV == 'local') {
    nullableEnvVars.push(...['GCS_UPLOAD_BUCKET'])
  }

  const parse = envParser(process.env)
  const pg = {
    host: parse('PG_HOST'),
    port: parseInt(parse('PG_PORT'), 10),
    userName: parse('PG_USER'),
    password: parse('PG_PASSWORD'),
    dbName: parse('PG_DB'),
    pool: {
      max: parseInt(parse('PG_POOL_MAX'), 10),
    },
    replication: parse('PG_REPLICATION') === 'true',
    replica: {
      host: parse('PG_REPLICA_HOST'),
      port: parseInt(parse('PG_REPLICA_PORT'), 10),
      userName: parse('PG_REPLICA_USER'),
      password: parse('PG_REPLICA_PASSWORD'),
      dbName: parse('PG_REPLICA_DB'),
    },
  }
  const server = {
    jwtSecret: parse('JWT_SECRET'),
    ssoJwtSecret: parse('SSO_JWT_SECRET'),
    gateway_url: parse('GATEWAY_URL'),
    apiEnv: parse('API_ENV'),
    instanceId:
      parse('GAE_INSTANCE') || `x${os.userInfo().username}_${os.hostname()}`,
    trustProxy: parse('TRUST_PROXY') === 'true',
    internalApiUrl: parse('INTERNAL_API_URL'),
  }
  const client = {
    url: parse('CLIENT_URL'),
    api_url: parse('API_PUBLIC_URL'),
  }
  const google = {
    auth: {
      iosClientId: parse('GAUTH_IOS_CLIENT_ID'),
      androidClientId: parse('GAUTH_ANDROID_CLIENT_ID'),
      clientId: parse('GAUTH_CLIENT_ID'),
      secret: parse('GAUTH_SECRET'),
    },
  }
  const posthog = {
    apiKey: parse('POSTHOG_API_KEY'),
  }
  const intercom = {
    token: parse('INTERCOM_TOKEN'),
    secretKey: parse('INTERCOM_SECRET_KEY'),
  }
  const sentry = {
    dsn: parse('SENTRY_DSN'),
  }
  const jaeger = {
    host: parse('JAEGER_HOST'),
  }
  const dev = {
    isLocal: parse('API_ENV') == 'local',
  }
  const queue = {
    location: parse('PUPPETEER_QUEUE_LOCATION'),
    name: parse('PUPPETEER_QUEUE_NAME'),
    contentFetchUrl: parse('CONTENT_FETCH_URL'),
    contentFetchGCFUrl: parse('CONTENT_FETCH_GCF_URL'),
    reminderTaskHandlerUrl: parse('REMINDER_TASK_HANDLER_URL'),
    integrationTaskHandlerUrl: parse('INTEGRATION_TASK_HANDLER_URL'),
    textToSpeechTaskHandlerUrl: parse('TEXT_TO_SPEECH_TASK_HANDLER_URL'),
    recommendationTaskHandlerUrl: parse('RECOMMENDATION_TASK_HANDLER_URL'),
    thumbnailTaskHandlerUrl: parse('THUMBNAIL_TASK_HANDLER_URL'),
    integrationExporterUrl: parse('INTEGRATION_EXPORTER_URL'),
    integrationImporterUrl: parse('INTEGRATION_IMPORTER_URL'),
    importerMetricsUrl: parse('IMPORTER_METRICS_COLLECTOR_URL'),
  }
  const imageProxy = {
    url: parse('IMAGE_PROXY_URL'),
    secretKey: parse('IMAGE_PROXY_SECRET'),
  }
  const twitter = {
    token: parse('TWITTER_BEARER_TOKEN'),
  }
  const fileUpload = {
    gcsUploadBucket: parse('GCS_UPLOAD_BUCKET'),
    gcsUploadSAKeyFilePath: parse('GCS_UPLOAD_SA_KEY_FILE_PATH'),
    gcsUploadPrivateBucket: parse('GCS_UPLOAD_PRIVATE_BUCKET'),
    dailyUploadLimit: parse('GCS_UPLOAD_DAILY_LIMIT')
      ? parseInt(parse('GCS_UPLOAD_DAILY_LIMIT'), 10)
      : 5, // default to 5
  }
  const sender = {
    message: parse('SENDER_MESSAGE'),
    feedback: parse('SENDER_FEEDBACK'),
    general: parse('SENDER_GENERAL'),
  }

  const sendgrid = {
    confirmationTemplateId: parse('SENDGRID_CONFIRMATION_TEMPLATE_ID'),
    reminderTemplateId: parse('SENDGRID_REMINDER_TEMPLATE_ID'),
    resetPasswordTemplateId: parse('SENDGRID_RESET_PASSWORD_TEMPLATE_ID'),
    installationTemplateId: parse('SENDGRID_INSTALLATION_TEMPLATE_ID'),
    verificationTemplateId: parse('SENDGRID_VERIFICATION_TEMPLATE_ID'),
  }

  const readwise = {
    apiUrl: parse('READWISE_API_URL'),
  }

  const gcp = {
    location: parse('GCP_LOCATION'),
  }

  const pocket = {
    consumerKey: parse('POCKET_CONSUMER_KEY'),
  }

  const subscription = {
    feed: {
      max: parse('SUBSCRIPTION_FEED_MAX')
        ? parseInt(parse('SUBSCRIPTION_FEED_MAX'), 10)
        : 256, // default to 256
    },
  }
  const redis = {
    mq: {
      url: parse('MQ_REDIS_URL'),
      cert: parse('MQ_REDIS_CERT')?.replace(/\\n/g, '\n'), // replace \n with new line
    },
    cache: {
      url: parse('REDIS_URL'),
      cert: parse('REDIS_CERT')?.replace(/\\n/g, '\n'), // replace \n with new line
    },
  }
  const notion = {
    clientId: parse('NOTION_CLIENT_ID'),
    clientSecret: parse('NOTION_CLIENT_SECRET'),
    authUrl: parse('NOTION_AUTH_URL'),
  }
  const score = {
    apiUrl: parse('SCORE_API_URL') || 'http://digest-score/batch',
  }

  return {
    pg,
    client,
    server,
    google,
    posthog,
    intercom,
    sentry,
    jaeger,
    imageProxy,
    twitter,
    dev,
    fileUpload,
    queue,
    sender,
    sendgrid,
    readwise,
    gcp,
    pocket,
    subscription,
    redis,
    notion,
    score,
  }
}

export type Merge<
  Target extends Record<string, any>,
  Part extends Record<string, any>
> = Omit<Target, keyof Part> & Part

/**
 * Make all properties in T optional
 * This is similar to TS's Partial type, but it also allows null
 */
export type Partialize<T> = {
  [P in keyof T]?: T[P] | null
}

export function exclude<A extends readonly any[], B extends readonly any[]>(
  a: A,
  b: B
): readonly Exclude<A[number], B[number]>[] {
  return a.filter((x) => b.includes(x)) as any
}

export type PickTuple<A, B extends readonly any[]> = Pick<A, B[number]>

export const env = getEnv()

export function homePageURL(): string {
  return env.client.url
}
