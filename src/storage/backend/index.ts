import { StorageBackendAdapter } from './adapter'
import { FileBackend } from './file'
import { S3Backend, S3ClientOptions } from './s3/adapter'
import { GCSBackend, GCSClientOptions } from './gcs/adapter'
import { getConfig, StorageBackendType } from '../../config'

export * from './s3'
export * from './gcs'
export * from './file'
export * from './adapter'

const {
  storageS3Region,
  storageS3Endpoint,
  storageS3ForcePathStyle,
  storageS3ClientTimeout,
  storageGcsProjectId,
  storageGcsKeyFilePath,
  storageGcsCredentials,
  storageGcsUseAdc,
  s3ProtocolAccessKeyId,
  s3ProtocolAccessKeySecret,
} = getConfig()

type ConfigForStorage<Type extends StorageBackendType> = Type extends 's3'
  ? S3ClientOptions
  : Type extends 'gcs'
  ? GCSClientOptions
  : undefined

export function createStorageBackend<Type extends StorageBackendType>(
  type: Type,
  config?: ConfigForStorage<Type>
) {
  let storageBackend: StorageBackendAdapter

  if (type === 'file') {
    storageBackend = new FileBackend()
  } else if (type === 'gcs') {
    const defaultOptions: GCSClientOptions = {
      projectId: storageGcsProjectId,
      keyFilePath: storageGcsKeyFilePath,
      credentials: storageGcsCredentials,
      useApplicationDefaultCredentials: storageGcsUseAdc,
      requestTimeout: storageS3ClientTimeout, // Reuse S3 timeout config
      // Reuse S3 protocol access keys as HMAC keys for GCS XML API
      accessKey: s3ProtocolAccessKeyId,
      secretKey: s3ProtocolAccessKeySecret,
      ...(config ? config : {}),
    }
    storageBackend = new GCSBackend(defaultOptions)
  } else {
    const defaultOptions: S3ClientOptions = {
      region: storageS3Region,
      endpoint: storageS3Endpoint,
      forcePathStyle: storageS3ForcePathStyle,
      requestTimeout: storageS3ClientTimeout,
      ...(config ? config : {}),
    }
    storageBackend = new S3Backend(defaultOptions)
  }

  return storageBackend
}
