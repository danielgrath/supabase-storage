import {
  Storage,
  Bucket,
  File,
  GetSignedUrlConfig,
  CreateResumableUploadOptions,
  TransferManager,
} from '@google-cloud/storage'
import { GoogleAuth } from 'google-auth-library'
import {
  S3Client,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  UploadPartCopyCommand,
} from '@aws-sdk/client-s3'
import { NodeHttpHandler } from '@smithy/node-http-handler'
import {
  StorageBackendAdapter,
  BrowserCacheHeaders,
  ObjectMetadata,
  ObjectResponse,
  UploadPart,
} from './../adapter'
import { ERRORS, StorageBackendError } from '@internal/errors'
import { getConfig } from '../../../config'
import { Readable } from 'node:stream'
import { createAgent, InstrumentedAgent } from '@internal/http'
import { monitorStream } from '@internal/streams'
import { BackupObjectInfo, ObjectBackup } from '@storage/backend/gcs/backup'

const { tracingFeatures, storageS3MaxSockets, tracingEnabled } = getConfig()

export interface GCSClientOptions {
  projectId?: string
  keyFilePath?: string
  credentials?: string | object
  httpAgent?: InstrumentedAgent
  requestTimeout?: number
  // HMAC keys for GCS XML API (multipart uploads) - reuse S3 protocol keys
  accessKey?: string
  secretKey?: string
  // ADC support
  useApplicationDefaultCredentials?: boolean
}

/**
 * GCSBackend
 * Interacts with Google Cloud Storage using a hybrid approach:
 *
 * 1. Native GCS SDK for most operations (getObject, uploadObject, etc.)
 *    - Supports GCS-specific features like generation-based versioning
 *    - Efficient streaming and metadata handling
 *
 * 2. AWS S3 SDK via GCS XML API for multipart operations
 *    - Full S3 multipart protocol compatibility
 *    - Works with existing S3 routes without changes
 *    - Uses HMAC keys for authentication
 *
 * Configuration Requirements:
 * - For GCS operations: Service account key file, inline credentials, or ADC
 * - For multipart operations: HMAC access key and secret key
 *
 * Authentication Methods (following Google Cloud best practices):
 * 1. useApplicationDefaultCredentials=true - Uses pure ADC (recommended for production)
 * 2. keyFilePath - Service account key file path (for development/testing)
 * 3. credentials - Inline service account credentials (for containerized apps)
 * 4. Automatic ADC fallback - Lets client library auto-detect (Google's recommended default)
 *
 * Environment Variables:
 * - STORAGE_GCS_PROJECT_ID (optional)
 * - STORAGE_GCS_KEY_FILE_PATH or STORAGE_GCS_CREDENTIALS (if not using ADC)
 * - STORAGE_GCS_USE_ADC=true (to explicitly enable ADC)
 * - S3_PROTOCOL_ACCESS_KEY_ID (HMAC access key)
 * - S3_PROTOCOL_ACCESS_KEY_SECRET (HMAC secret key)
 *
 * For Workload Identity (recommended for GKE/Cloud Run):
 * - Set useApplicationDefaultCredentials=true or leave credentials empty
 * - Ensure your K8s service account is bound to a GCP service account
 * - No need for explicit key files or credential strings
 */
export class GCSBackend implements StorageBackendAdapter {
  client: Storage
  agent: InstrumentedAgent
  s3Client: S3Client

  constructor(options: GCSClientOptions) {
    this.agent =
      options.httpAgent ??
      createAgent('gcs_default', {
        maxSockets: storageS3MaxSockets, // Reuse S3 max sockets config
      })

    if (this.agent.httpsAgent && tracingEnabled) {
      this.agent.monitor()
    }

    // Default client for API operations
    this.client = this.createGCSClient({
      ...options,
      name: 'gcs_default',
      httpAgent: this.agent,
    })

    // S3 client for multipart operations via GCS XML API
    this.s3Client = new S3Client({
      region: 'auto', // GCS XML API uses 'auto' region
      endpoint: 'https://storage.googleapis.com', // GCS XML API endpoint
      credentials: {
        accessKeyId: options.accessKey || '',
        secretAccessKey: options.secretKey || '',
      },
      requestHandler: new NodeHttpHandler({
        httpAgent: this.agent.httpAgent,
        httpsAgent: this.agent.httpsAgent,
        connectionTimeout: 5000,
        requestTimeout: options.requestTimeout || 30000,
      }),
      forcePathStyle: true, // GCS XML API requires path-style access
    })
  }

  /**
   * Gets an object body and metadata
   * @param bucketName
   * @param key
   * @param version
   * @param headers
   * @param signal
   */
  async getObject(
    bucketName: string,
    key: string,
    version: string | undefined,
    headers?: BrowserCacheHeaders,
    signal?: AbortSignal
  ): Promise<ObjectResponse> {
    try {
      const bucket = this.client.bucket(bucketName)
      // Use GCS generation for versioning instead of encoding in filename
      const fileOptions = version ? { generation: Number(version) } : undefined
      const file = bucket.file(key, fileOptions)

      const readStreamOptions: any = {}
      let isRangeRequest = false

      if (headers?.range) {
        isRangeRequest = true
        const range = headers.range.replace('bytes=', '')
        const [start, end] = range.split('-').map((n) => (n ? parseInt(n) : undefined))
        if (start !== undefined) readStreamOptions.start = start
        if (end !== undefined) readStreamOptions.end = end
      }

      const readStream = file.createReadStream(readStreamOptions)

      // Handle abort signal
      if (signal) {
        signal.addEventListener('abort', () => {
          readStream.destroy()
        })
      }

      // Get metadata first (needed for conditional headers check)
      // Note: This adds an extra round-trip but GCS Node.js client doesn't support
      // conditional headers on createReadStream yet
      const [metadata] = await file.getMetadata()

      // Check conditional headers
      if (headers?.ifNoneMatch && headers.ifNoneMatch === metadata.etag) {
        return {
          metadata: {
            cacheControl: metadata.cacheControl || 'no-cache',
            mimetype: metadata.contentType || 'application/octet-stream',
            eTag: metadata.etag || '', // Note: GCS ETags are not guaranteed to be MD5
            lastModified: metadata.updated ? new Date(metadata.updated) : undefined,
            contentLength: parseInt(String(metadata.size)) || 0,
            size: parseInt(String(metadata.size)) || 0,
            httpStatusCode: 304,
          },
          httpStatusCode: 304,
          body: undefined,
        }
      }

      if (headers?.ifModifiedSince) {
        const ifModifiedSince = new Date(headers.ifModifiedSince)
        const lastModified = metadata.updated ? new Date(metadata.updated) : new Date()
        if (lastModified <= ifModifiedSince) {
          return {
            metadata: {
              cacheControl: metadata.cacheControl || 'no-cache',
              mimetype: metadata.contentType || 'application/octet-stream',
              eTag: metadata.etag || '', // Note: GCS ETags are not guaranteed to be MD5
              lastModified: lastModified,
              contentLength: parseInt(String(metadata.size)) || 0,
              size: parseInt(String(metadata.size)) || 0,
              httpStatusCode: 304,
            },
            httpStatusCode: 304,
            body: undefined,
          }
        }
      }

      // Calculate content length for range requests
      let contentLength = parseInt(String(metadata.size)) || 0
      let contentRange: string | undefined

      if (isRangeRequest && readStreamOptions.start !== undefined) {
        const start = readStreamOptions.start
        const end = readStreamOptions.end ?? contentLength - 1
        contentLength = end - start + 1
        contentRange = `bytes ${start}-${end}/${metadata.size}`
      }

      return {
        metadata: {
          cacheControl: metadata.cacheControl || 'no-cache',
          mimetype: metadata.contentType || 'application/octet-stream',
          eTag: metadata.etag || '', // Note: GCS ETags are not guaranteed to be MD5
          lastModified: metadata.updated ? new Date(metadata.updated) : undefined,
          contentRange: contentRange,
          contentLength: contentLength,
          size: parseInt(String(metadata.size)) || 0,
          httpStatusCode: isRangeRequest ? 206 : 200, // Return 206 for partial content
        },
        httpStatusCode: isRangeRequest ? 206 : 200,
        body: readStream,
      }
    } catch (err: any) {
      if (err.code === 404) {
        throw ERRORS.NoSuchKey(key)
      }
      throw StorageBackendError.fromError(err)
    }
  }

  /**
   * Uploads and store an object
   * @param bucketName
   * @param key
   * @param version
   * @param body
   * @param contentType
   * @param cacheControl
   * @param signal
   */
  async uploadObject(
    bucketName: string,
    key: string,
    version: string | undefined,
    body: Readable,
    contentType: string,
    cacheControl: string,
    signal?: AbortSignal
  ): Promise<ObjectMetadata> {
    if (signal?.aborted) {
      throw ERRORS.Aborted('Upload was aborted')
    }

    try {
      const bucket = this.client.bucket(bucketName)
      // Note: For uploads, version/generation is typically assigned by GCS, not specified
      const file = bucket.file(key)

      const dataStream = tracingFeatures?.upload ? monitorStream(body) : body

      const writeStream = file.createWriteStream({
        metadata: {
          contentType,
          cacheControl,
        },
        resumable: false, // Use simple upload for better performance on smaller files
      })

      // Handle abort signal
      if (signal) {
        signal.addEventListener(
          'abort',
          () => {
            writeStream.destroy()
          },
          { once: true }
        )
      }

      // Pipe the data and wait for completion
      dataStream.pipe(writeStream)

      await new Promise<void>((resolve, reject) => {
        writeStream.on('error', reject)
        writeStream.on('finish', resolve)
      })

      // Get the metadata after upload
      const metadata = await this.headObject(bucketName, key, version)

      return metadata
    } catch (err: any) {
      if (err.name === 'AbortError' || signal?.aborted) {
        throw ERRORS.AbortedTerminate('Upload was aborted', err)
      }
      throw StorageBackendError.fromError(err)
    }
  }

  /**
   * Deletes an object
   * @param bucket
   * @param key
   * @param version
   */
  async deleteObject(bucket: string, key: string, version: string | undefined): Promise<void> {
    try {
      const bucketRef = this.client.bucket(bucket)
      const fileOptions = version ? { generation: Number(version) } : undefined
      const file = bucketRef.file(key, fileOptions)
      await file.delete()
    } catch (err: any) {
      if (err.code !== 404) {
        throw StorageBackendError.fromError(err)
      }
      // Ignore 404 errors for delete operations
    }
  }

  /**
   * Copies an existing object to the given location
   * @param bucket
   * @param source
   * @param version
   * @param destination
   * @param destinationVersion
   * @param metadata
   * @param conditions
   */
  async copyObject(
    bucket: string,
    source: string,
    version: string | undefined,
    destination: string,
    destinationVersion: string | undefined,
    metadata?: { cacheControl?: string; mimetype?: string },
    conditions?: {
      ifMatch?: string
      ifNoneMatch?: string
      ifModifiedSince?: Date
      ifUnmodifiedSince?: Date
    }
  ): Promise<Pick<ObjectMetadata, 'httpStatusCode' | 'eTag' | 'lastModified'>> {
    try {
      const bucketRef = this.client.bucket(bucket)
      const sourceFileOptions = version ? { generation: Number(version) } : undefined
      const sourceFile = bucketRef.file(source, sourceFileOptions)
      const destinationFile = bucketRef.file(destination)

      const copyOptions: any = {}

      if (metadata) {
        copyOptions.metadata = {}
        if (metadata.mimetype) copyOptions.metadata.contentType = metadata.mimetype
        if (metadata.cacheControl) copyOptions.metadata.cacheControl = metadata.cacheControl
      }

      // Apply preconditions
      if (conditions?.ifMatch) {
        copyOptions.srcPreconditions = { ifGenerationMatch: conditions.ifMatch }
      }
      if (conditions?.ifNoneMatch) {
        copyOptions.srcPreconditions = { ifGenerationNotMatch: conditions.ifNoneMatch }
      }
      if (conditions?.ifModifiedSince) {
        copyOptions.srcPreconditions = { ifMetagenerationMatch: conditions.ifModifiedSince }
      }
      if (conditions?.ifUnmodifiedSince) {
        copyOptions.srcPreconditions = { ifMetagenerationNotMatch: conditions.ifUnmodifiedSince }
      }

      await sourceFile.copy(destinationFile, copyOptions)

      // Get metadata of the copied file
      const [copiedMetadata] = await destinationFile.getMetadata()

      return {
        httpStatusCode: 200,
        eTag: copiedMetadata.etag || '',
        lastModified: copiedMetadata.updated ? new Date(copiedMetadata.updated) : undefined,
      }
    } catch (err: any) {
      throw StorageBackendError.fromError(err)
    }
  }

  async list(
    bucket: string,
    options?: {
      prefix?: string
      delimiter?: string
      nextToken?: string
      startAfter?: string
      beforeDate?: Date
    }
  ): Promise<{ keys: { name: string; size: number }[]; nextToken?: string }> {
    try {
      const bucketRef = this.client.bucket(bucket)

      const listOptions: any = {}
      if (options?.prefix) listOptions.prefix = options.prefix
      if (options?.delimiter) listOptions.delimiter = options.delimiter
      if (options?.nextToken) listOptions.pageToken = options.nextToken
      if (options?.startAfter) listOptions.startOffset = options.startAfter

      const [files, nextQuery] = await bucketRef.getFiles(listOptions)

      let filteredFiles = files
      if (options?.beforeDate) {
        filteredFiles = files.filter((file) => {
          const metadata = file.metadata
          if (metadata.timeCreated) {
            const fileDate = new Date(metadata.timeCreated)
            return fileDate < options.beforeDate!
          }
          return false
        })
      }

      const keys = filteredFiles.map((file) => {
        let name = file.name
        if (options?.prefix) {
          name = name.replace(options.prefix, '').replace('/', '')
        }

        return {
          name: name,
          size: parseInt(String(file.metadata.size)) || 0,
        }
      })

      return {
        keys,
        nextToken: nextQuery?.pageToken,
      }
    } catch (err: any) {
      throw StorageBackendError.fromError(err)
    }
  }

  /**
   * Deletes multiple objects
   * @param bucket
   * @param prefixes
   */
  async deleteObjects(bucket: string, prefixes: string[]): Promise<void> {
    try {
      const bucketRef = this.client.bucket(bucket)

      // Delete files in parallel
      const deletePromises = prefixes.map(async (prefix) => {
        try {
          const file = bucketRef.file(prefix)
          await file.delete()
        } catch (err: any) {
          // Ignore 404 errors for delete operations
          if (err.code !== 404) {
            throw err
          }
        }
      })

      await Promise.all(deletePromises)
    } catch (err: any) {
      throw StorageBackendError.fromError(err)
    }
  }

  /**
   * Returns metadata information of a specific object
   * @param bucket
   * @param key
   * @param version
   */
  async headObject(
    bucket: string,
    key: string,
    version: string | undefined
  ): Promise<ObjectMetadata> {
    try {
      const bucketRef = this.client.bucket(bucket)
      const fileOptions = version ? { generation: Number(version) } : undefined
      const file = bucketRef.file(key, fileOptions)

      const [metadata] = await file.getMetadata()

      return {
        cacheControl: metadata.cacheControl || 'no-cache',
        mimetype: metadata.contentType || 'application/octet-stream',
        eTag: metadata.etag || '', // Note: GCS ETags are not guaranteed to be MD5
        lastModified: metadata.updated ? new Date(metadata.updated) : undefined,
        contentLength: parseInt(String(metadata.size)) || 0,
        httpStatusCode: 200,
        size: parseInt(String(metadata.size)) || 0,
      }
    } catch (err: any) {
      if (err.code === 404) {
        throw ERRORS.NoSuchKey(key)
      }
      throw StorageBackendError.fromError(err)
    }
  }

  /**
   * Returns a private url that can only be accessed internally by the system
   * @param bucket
   * @param key
   * @param version
   */
  async privateAssetUrl(bucket: string, key: string, version: string | undefined): Promise<string> {
    try {
      const bucketRef = this.client.bucket(bucket)
      const fileOptions = version ? { generation: Number(version) } : undefined
      const file = bucketRef.file(key, fileOptions)

      const config: GetSignedUrlConfig = {
        version: 'v4',
        action: 'read',
        expires: Date.now() + 10 * 60 * 1000, // 10 minutes
      }

      const [signedUrl] = await file.getSignedUrl(config)
      return signedUrl
    } catch (err: any) {
      throw StorageBackendError.fromError(err)
    }
  }

  async createMultiPartUpload(
    bucketName: string,
    key: string,
    version: string | undefined,
    contentType: string,
    cacheControl: string
  ): Promise<string> {
    try {
      const command = new CreateMultipartUploadCommand({
        Bucket: bucketName,
        Key: key,
        ContentType: contentType,
        CacheControl: cacheControl,
        Metadata: {
          Version: version || '',
        },
      })

      const response = await this.s3Client.send(command)

      if (!response.UploadId) {
        throw ERRORS.InvalidUploadId()
      }

      return response.UploadId
    } catch (err: any) {
      throw StorageBackendError.fromError(err)
    }
  }

  async uploadPart(
    bucketName: string,
    key: string,
    version: string,
    uploadId: string,
    partNumber: number,
    body?: string | Uint8Array | Buffer | Readable,
    length?: number,
    signal?: AbortSignal
  ): Promise<{ ETag?: string }> {
    try {
      const command = new UploadPartCommand({
        Bucket: bucketName,
        Key: key,
        UploadId: uploadId,
        PartNumber: partNumber,
        Body: body,
        ContentLength: length,
      })

      const response = await this.s3Client.send(command, {
        abortSignal: signal,
      })

      return {
        ETag: response.ETag,
      }
    } catch (err: any) {
      if (err.name === 'AbortError' || signal?.aborted) {
        throw ERRORS.AbortedTerminate('Upload was aborted', err)
      }
      throw StorageBackendError.fromError(err)
    }
  }

  async completeMultipartUpload(
    bucketName: string,
    key: string,
    uploadId: string,
    version: string,
    parts: UploadPart[]
  ): Promise<
    Omit<UploadPart, 'PartNumber'> & { location?: string; bucket?: string; version: string }
  > {
    try {
      const command = new CompleteMultipartUploadCommand({
        Bucket: bucketName,
        Key: key,
        UploadId: uploadId,
        MultipartUpload: {
          Parts: parts,
        },
      })

      const response = await this.s3Client.send(command)

      return {
        version: version,
        location: response.Location,
        bucket: bucketName,
        ETag: response.ETag,
      }
    } catch (err: any) {
      throw StorageBackendError.fromError(err)
    }
  }

  async abortMultipartUpload(bucketName: string, key: string, uploadId: string): Promise<void> {
    try {
      const command = new AbortMultipartUploadCommand({
        Bucket: bucketName,
        Key: key,
        UploadId: uploadId,
      })

      await this.s3Client.send(command)
    } catch (err: any) {
      throw StorageBackendError.fromError(err)
    }
  }

  async uploadPartCopy(
    bucketName: string,
    key: string,
    version: string,
    uploadId: string,
    partNumber: number,
    sourceKey: string,
    sourceKeyVersion?: string,
    bytesRange?: { fromByte: number; toByte: number }
  ): Promise<{ eTag?: string; lastModified?: Date }> {
    try {
      const command = new UploadPartCopyCommand({
        Bucket: bucketName,
        Key: key,
        UploadId: uploadId,
        PartNumber: partNumber,
        CopySource: `${bucketName}/${sourceKey}`,
        CopySourceRange: bytesRange
          ? `bytes=${bytesRange.fromByte}-${bytesRange.toByte}`
          : undefined,
      })

      const response = await this.s3Client.send(command)

      return {
        eTag: response.CopyPartResult?.ETag,
        lastModified: response.CopyPartResult?.LastModified,
      }
    } catch (err: any) {
      throw StorageBackendError.fromError(err)
    }
  }

  async backup(backupInfo: BackupObjectInfo): Promise<void> {
    return new ObjectBackup(this.client, backupInfo, this.s3Client).backup()
  }

  close(): void {
    this.agent.close()
  }

  /**
   * Helper method to perform chunked uploads using TransferManager
   * This is the recommended way to upload large files to GCS
   *
   * @param bucketName - The bucket to upload to
   * @param key - The destination object name
   * @param filePath - Path to the local file to upload
   * @param options - Upload options
   * @returns Promise that resolves when upload is complete
   */
  async uploadFileInChunks(
    bucketName: string,
    key: string,
    filePath: string,
    options?: {
      chunkSizeBytes?: number
      concurrencyLimit?: number
      contentType?: string
      cacheControl?: string
    }
  ): Promise<void> {
    const bucket = this.client.bucket(bucketName)
    const transferManager = new TransferManager(bucket)

    await transferManager.uploadFileInChunks(filePath, {
      uploadName: key,
      chunkSizeBytes: options?.chunkSizeBytes || 32 * 1024 * 1024, // Default 32MB
      concurrencyLimit: options?.concurrencyLimit || 4,
      headers: {
        'Content-Type': options?.contentType || 'application/octet-stream',
        'Cache-Control': options?.cacheControl || 'no-cache',
      },
    })
  }

  protected createGCSClient(options: GCSClientOptions & { name: string }): Storage {
    const clientOptions: any = {}

    // Set project ID if provided (recommended but optional with ADC)
    if (options.projectId) {
      clientOptions.projectId = options.projectId
    }

    // Handle authentication according to Google Cloud best practices
    if (options.useApplicationDefaultCredentials === true) {
      // Explicitly use ADC - don't set any auth options, let the client library handle it
      console.log(
        'GCS Backend: Using Application Default Credentials (ADC) - letting client library auto-detect'
      )
    } else if (options.keyFilePath) {
      // Use service account key file
      clientOptions.keyFilename = options.keyFilePath
      console.log(`GCS Backend: Using service account key file: ${options.keyFilePath}`)
    } else if (options.credentials) {
      // Handle credentials as JSON string or object
      if (typeof options.credentials === 'string') {
        try {
          clientOptions.credentials = JSON.parse(options.credentials)
          console.log('GCS Backend: Using inline service account credentials (JSON string)')
        } catch {
          // If parsing fails, assume it's a file path
          clientOptions.keyFilename = options.credentials
          console.log(
            `GCS Backend: Using service account key file from string: ${options.credentials}`
          )
        }
      } else {
        clientOptions.credentials = options.credentials
        console.log('GCS Backend: Using inline service account credentials (object)')
      }
    } else {
      // No explicit credentials - let ADC handle it (Google's recommended approach)
      // The Storage client will automatically use ADC per Google's documentation
      console.log(
        'GCS Backend: No explicit credentials, using Application Default Credentials (recommended)'
      )
    }

    // Configure timeout if provided
    if (options.requestTimeout) {
      clientOptions.timeout = options.requestTimeout
    }

    // Note: Custom HTTP agent configuration is skipped for simplicity
    // Google Cloud client libraries handle connection pooling and agents internally

    return new Storage(clientOptions)
  }
}
