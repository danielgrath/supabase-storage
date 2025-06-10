import { Storage, File } from '@google-cloud/storage'
import {
  S3Client,
  CopyObjectCommand,
  CreateMultipartUploadCommand,
  UploadPartCopyCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  CompletedPart,
} from '@aws-sdk/client-s3'

const FIVE_GB = 5 * 1024 * 1024 * 1024

export interface BackupObjectInfo {
  // Source Object Details
  sourceBucket: string
  sourceKey: string

  // Destination Object Details
  destinationBucket: string
  destinationKey: string

  size: number
}

/**
 * Class representing an object backup operation between GCS buckets.
 * Uses a hybrid approach:
 * - Native GCS copy for files ≤5GB (efficient single operation)
 * - S3-compatible multipart copy via GCS XML API for large files >5GB (up to 300% faster)
 */
export class ObjectBackup {
  private static readonly MAX_PART_SIZE = 5 * 1024 * 1024 * 1024 // 5 GB per part
  private static readonly MAX_CONCURRENT_UPLOADS = 5 // Adjust based on your system's capabilities
  private gcsClient: Storage
  private s3Client?: S3Client // Optional S3 client for multipart operations
  private objectInfo: BackupObjectInfo

  /**
   * Creates an instance of ObjectBackup.
   * @param gcsClient - An instance of GCS Storage client.
   * @param objectInfo - Information about the object to be backed up.
   * @param s3Client - Optional S3 client for multipart operations via GCS XML API.
   */
  constructor(gcsClient: Storage, objectInfo: BackupObjectInfo, s3Client?: S3Client) {
    this.gcsClient = gcsClient
    this.s3Client = s3Client
    this.objectInfo = objectInfo
  }

  /**
   * Initiates the backup (copy) process for the specified object.
   * Automatically chooses the best strategy based on file size.
   */
  public async backup(): Promise<void> {
    try {
      const { size } = this.objectInfo

      if (size > FIVE_GB && this.s3Client) {
        // Use S3-compatible multipart copy for large files
        // This can provide up to 300% performance improvement
        await this.multipartCopy()
      } else {
        // Use native GCS copy for smaller files or when S3 client is not available
        // GCS handles large files automatically with resumable uploads under the hood
        await this.singleCopy()
      }
    } catch (error) {
      throw error
    }
  }

  /**
   * Performs a single copy operation using GCS's native copy functionality.
   * GCS handles large files automatically using resumable uploads under the hood.
   */
  private async singleCopy(): Promise<void> {
    const { sourceBucket, sourceKey, destinationBucket, destinationKey } = this.objectInfo

    const sourceBucketRef = this.gcsClient.bucket(sourceBucket)
    const sourceFile = sourceBucketRef.file(sourceKey)

    const destinationBucketRef = this.gcsClient.bucket(destinationBucket)
    const destinationFile = destinationBucketRef.file(destinationKey)

    // GCS copy operation - this handles large files automatically
    await sourceFile.copy(destinationFile)
  }

  /**
   * Performs a multipart copy operation using the S3-compatible XML API for objects > 5GB.
   * This can provide significant performance improvements for large files.
   */
  private async multipartCopy(): Promise<void> {
    if (!this.s3Client) {
      throw new Error('S3 client is required for multipart copy operations')
    }

    const { sourceBucket, sourceKey, destinationBucket, destinationKey, size } = this.objectInfo

    // Step 1: Initiate Multipart Upload via GCS XML API
    const createMultipartUploadCommand = new CreateMultipartUploadCommand({
      Bucket: destinationBucket,
      Key: destinationKey,
    })

    const createMultipartUploadResponse = await this.s3Client.send(createMultipartUploadCommand)
    const uploadId = createMultipartUploadResponse.UploadId

    if (!uploadId) {
      throw new Error('Failed to initiate multipart upload.')
    }

    const maxPartSize = ObjectBackup.MAX_PART_SIZE
    const numParts = Math.ceil(size / maxPartSize)
    const completedParts: CompletedPart[] = []

    try {
      // Step 2: Copy Parts Concurrently
      await this.copyPartsConcurrently(uploadId, numParts, size, completedParts)

      // Step 3: Sort the completed parts by PartNumber
      completedParts.sort((a, b) => (a.PartNumber! < b.PartNumber! ? -1 : 1))

      // Step 4: Complete Multipart Upload
      const completeMultipartUploadCommand = new CompleteMultipartUploadCommand({
        Bucket: destinationBucket,
        Key: destinationKey,
        UploadId: uploadId,
        MultipartUpload: {
          Parts: completedParts,
        },
      })

      await this.s3Client.send(completeMultipartUploadCommand)
    } catch (error) {
      // Abort the multipart upload in case of failure
      const abortMultipartUploadCommand = new AbortMultipartUploadCommand({
        Bucket: destinationBucket,
        Key: destinationKey,
        UploadId: uploadId,
      })

      await this.s3Client.send(abortMultipartUploadCommand)
      throw error
    }
  }

  /**
   * Copies parts of the object concurrently using the S3-compatible XML API.
   * @param uploadId - The UploadId from the initiated multipart upload.
   * @param numParts - Total number of parts to copy.
   * @param size - Total size of the object in bytes.
   * @param completedParts - Array to store completed parts information.
   */
  private async copyPartsConcurrently(
    uploadId: string,
    numParts: number,
    size: number,
    completedParts: CompletedPart[]
  ): Promise<void> {
    if (!this.s3Client) {
      throw new Error('S3 client is required for multipart copy operations')
    }

    const { sourceBucket, sourceKey, destinationBucket, destinationKey } = this.objectInfo
    const partSize = ObjectBackup.MAX_PART_SIZE
    let currentPart = 1

    // Worker function to copy a single part
    const copyPart = async (partNumber: number): Promise<void> => {
      const start = (partNumber - 1) * partSize
      const end = partNumber * partSize < size ? partNumber * partSize - 1 : size - 1

      const uploadPartCopyCommand = new UploadPartCopyCommand({
        Bucket: destinationBucket,
        Key: destinationKey,
        PartNumber: partNumber,
        UploadId: uploadId,
        CopySource: encodeURIComponent(`${sourceBucket}/${sourceKey}`),
        CopySourceRange: `bytes=${start}-${end}`,
      })

      const uploadPartCopyResponse = await this.s3Client!.send(uploadPartCopyCommand)

      if (!uploadPartCopyResponse.CopyPartResult?.ETag) {
        throw new Error(`Failed to copy part ${partNumber}. No ETag returned.`)
      }

      completedParts.push({
        ETag: uploadPartCopyResponse.CopyPartResult.ETag,
        PartNumber: partNumber,
      })
    }

    // Array to hold active worker promises
    const workers: Promise<void>[] = []

    // Start concurrent workers
    for (let i = 0; i < ObjectBackup.MAX_CONCURRENT_UPLOADS && currentPart <= numParts; i++) {
      const worker = (async () => {
        while (currentPart <= numParts) {
          const partToCopy = currentPart
          currentPart += 1
          try {
            await copyPart(partToCopy)
          } catch (error) {
            throw error
          }
        }
      })()
      workers.push(worker)
    }

    // Wait for all workers to complete
    await Promise.all(workers)
  }
}
