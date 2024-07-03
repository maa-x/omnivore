/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import { File, GetSignedUrlConfig, Storage } from '@google-cloud/storage'
import axios from 'axios'
import { ContentReaderType } from '../entity/library_item'
import { env } from '../env'
import { PageType } from '../generated/graphql'
import { ContentFormat } from '../jobs/upload_content'
import { logger } from './logger'
import { storageService } from '@omnivore/utils'

export const contentReaderForLibraryItem = (
  itemType: string,
  uploadFileId: string | null | undefined
) => {
  if (!uploadFileId) {
    return ContentReaderType.WEB
  }
  switch (itemType) {
    case PageType.Book:
      return ContentReaderType.EPUB
    case PageType.File:
      return ContentReaderType.PDF
    default:
      return ContentReaderType.WEB
  }
}

// /* On GAE/Prod, we shall rely on default app engine service account credentials.
//  * Two changes needed: 1) add default service account to our uploads GCS Bucket
//  * with create and view access. 2) add 'Service Account Token Creator' role to
//  * the default app engine service account on the IAM page. We also need to
//  * enable IAM related APIs on the project.
//  */
export const storage = env.fileUpload?.gcsUploadSAKeyFilePath
  ? new Storage({ keyFilename: env.fileUpload.gcsUploadSAKeyFilePath })
  : new Storage()
const bucketName = env.fileUpload.gcsUploadBucket
const maxContentLength = 10 * 1024 * 1024 // 10MB

export const countOfFilesWithPrefix = async (prefix: string) => {
  const files = await storageService.getFiles(prefix);
  logger.info(`Files with prefix ${prefix}: ${files.length}`)
  return files.length
}

export const generateUploadSignedUrl = async (
  filePathName: string,
  contentType: string,
  selectedBucket?: string
): Promise<string> => {
  return storageService.getUploadSignedUrl(filePathName, {contentType: contentType, selectedBucket: selectedBucket})
}

export const generateDownloadSignedUrl = async (
  filePathName: string,
  config?: {
    expires?: number
  }
): Promise<string> => {
  return storageService.getDownloadSignedUrl(filePathName, config)
}

export const getStorageFileDetails = async (
  id: string,
  fileName: string
): Promise<{ md5Hash: string; fileUrl: string }> => {
  const filePathName = generateUploadFilePathName(id, fileName)
  return await storageService.getFileMetadata(filePathName)
}

export const generateUploadFilePathName = (
  id: string,
  fileName: string
): string => {
  return `u/${id}/${fileName}`
}

export const uploadToBucket = async (
  filePath: string,
  data: Buffer,
  options?: { contentType?: string; public?: boolean; timeout?: number },
  selectedBucket?: string
): Promise<void> => {
  await storageService.save(filePath, data, { timeout: 30000, ...options })
}

export const downloadFromUrl = async (
  contentObjUrl: string,
  timeout?: number
) => {
  // download the content as stream and max 10MB
  const response = await axios.get<Buffer>(contentObjUrl, {
    responseType: 'stream',
    maxContentLength,
    timeout,
  })

  return response.data
}

export const uploadToSignedUrl = async (
  uploadSignedUrl: string,
  data: Buffer,
  contentType: string,
  timeout?: number
) => {
  // upload the stream to the signed url
  await axios.put(uploadSignedUrl, data, {
    headers: {
      'Content-Type': contentType,
    },
    maxBodyLength: maxContentLength,
    timeout,
  })
}

export const isFileExists = async (filePath: string): Promise<boolean> => {
  return storageService.isFileExists(filePath)
}

export const downloadFromStorageService = async (filePath: string): Promise<Buffer> => {
  const file = await storageService.download(filePath)
  return Buffer.from(file)
}

export const contentFilePath = ({
  userId,
  libraryItemId,
  format,
  savedAt,
  updatedAt,
}: {
  userId: string
  libraryItemId: string
  format: ContentFormat
  savedAt?: Date
  updatedAt?: Date
}) => {
  // Use updatedAt for highlightedMarkdown format because highlights are saved
  const date = format === 'highlightedMarkdown' ? updatedAt : savedAt

  if (!date) {
    throw new Error('Date not found')
  }

  return `content/${userId}/${libraryItemId}.${date.getTime()}.${format}`
}
