import * as fs from 'fs';
import * as path from 'path';
import { createHmac, createHash } from 'crypto';
import { Storage, GetSignedUrlConfig } from '@google-cloud/storage';
import { storageServiceRouter } from './upload_file_route';
import { Buffer } from 'buffer';

const URL_EXPIRATION_SECONDS = 3600; // 1 hour for example

function generateSignature(payload: string, secretKey: string): string {
  return createHmac('sha256', secretKey).update(payload).digest('hex');
}

interface StorageService {
  save(filePath: string, data: Buffer, options?: { contentType?: string; timeout?: number }): Promise<void>;
  download(filePath: string): Promise<Buffer>;
  isFileExists(filePath: string): Promise<boolean>;
  getFiles(prefix?: string): Promise<string[]>;
  createFile(filePath: string): any;
  getUploadSignedUrl(filePathName: string, config?: { contentType?: string, selectedBucket?: string, expires?: number}): Promise<string>;
  getDownloadSignedUrl(filePathName: string, config?: { contentType?: string, selectedBucket?: string, expires?: number}): Promise<string>;
  getFileMetadata(fileName: string): Promise<{ md5Hash: string; fileUrl: string }>
}

class GCSStorageService implements StorageService {
  private storage: Storage;
  private bucketName: string;

  constructor(bucketName: string, keyFilename?: string) {
    this.storage = new Storage({ keyFilename });
    this.bucketName = bucketName;
  }

  async save(filePath: string, data: Buffer, options?: { contentType?: string; timeout?: number }): Promise<void> {
    const file = this.storage.bucket(this.bucketName).file(filePath);
    const saveOptions = {
      metadata: {
        contentType: options?.contentType || 'application/octet-stream',
      },
      timeout: options?.timeout,
    };
    await file.save(data, saveOptions);
  }

  async download(filePath: string): Promise<Buffer> {
    const file = this.storage.bucket(this.bucketName).file(filePath);
    const downloadResponse = await file.download();
    return downloadResponse[0];
  }

  async isFileExists(filePath: string): Promise<boolean> {
    const file = this.storage.bucket(this.bucketName).file(filePath);
    const existsResponse = await file.exists();
    return existsResponse[0];
  }

  async getFiles(prefix?: string): Promise<string[]> {
    prefix = prefix || '';
    const [files] = await this.storage.bucket(this.bucketName).getFiles({ prefix });
    return files.map(file => file.name);
  }

  createFile(filePath: string): any {
    return this.storage.bucket(this.bucketName).file(filePath);
  }

  async generateSignedUrl(filePathName: string, config? :{ contentType?: string, selectedBucket?: string, expires?: number}): Promise<string> {
    // These options will allow temporary uploading of file with requested content type
    const options: GetSignedUrlConfig = {
      version: 'v4',
      action: 'write',
      expires: config?.expires || Date.now() + 15 * 60 * 1000, // 15 minutes
      contentType: config?.contentType || 'application/octet-stream',
    }

    // Get a v4 signed URL for uploading file
    const [url] = await this.storage
      .bucket(config?.selectedBucket || this.bucketName)
      .file(filePathName)
      .getSignedUrl(options)
    return url
  }

  async getUploadSignedUrl(filePathName: string, config? :{ contentType?: string, selectedBucket?: string, expires?: number}): Promise<string> {
    return this.generateSignedUrl(filePathName, config);
  }

  async getDownloadSignedUrl(filePathName: string, config? :{ contentType?: string, selectedBucket?: string, expires?: number}): Promise<string> {
    return this.generateSignedUrl(filePathName, config);
  }
  
  // Inside GCSStorageService class
async getFileMetadata(fileName: string): Promise<{ md5Hash: string; fileUrl: string }> {
  const file = this.storage.bucket(this.bucketName).file(fileName);
  const [metadata] = await file.getMetadata();
  const md5Hash = Buffer.from(metadata.md5Hash || '', 'base64').toString('hex');
  const fileUrl = file.publicUrl();

  return { md5Hash, fileUrl };
}
}

class FileSystemStorageService implements StorageService {
  private baseDirectory: string;

  constructor(baseDirectory: string) {
    if (process.env.FS_UPLOAD_SECRET_KEY === undefined) {
      throw new Error('FS_UPLOAD_SECRET_KEY environment variable is not set');
    }
    this.baseDirectory = baseDirectory;
  }

  // Options are not used in FileSystemStorageService.save but are kept for compatibility with GCSStorageService
  async save(filePath: string, data: Buffer, _options?: { contentType?: string; timeout?: number }): Promise<void> {
    console.log("StorageService: save:", filePath)
    const fullFilePath = path.join(this.baseDirectory, filePath);
    fs.mkdirSync(path.dirname(fullFilePath), { recursive: true });
    fs.writeFileSync(fullFilePath, data);
  }

  async download(filePath: string): Promise<Buffer> {
    console.log("StorageService: download:", filePath)
    const fullFilePath = path.join(this.baseDirectory, filePath);
    return fs.promises.readFile(fullFilePath);
  }

  async isFileExists(filePath: string): Promise<boolean> {
    const fullFilePath = path.join(this.baseDirectory, filePath);
    try {
      await fs.promises.access(fullFilePath);
      return true;
    } catch {
      return false;
    }
  }

  async getFiles(prefix?: string): Promise<string[]> {
    console.log("StorageService: getFiles:", prefix)
    try {
      var filter = prefix || '';
      const baseDirectoryFiles = fs.readdirSync(this.baseDirectory);
      const filteredFiles = baseDirectoryFiles.filter(file => file.startsWith(filter));
      return filteredFiles || [];
    } catch (error) {
      console.error('Error reading directory:', error);
      return [];
    }
  }

  createFile(filePath: string): any {
    console.log("StorageService: createFile:", filePath)
    return path.join(this.baseDirectory, filePath);
  }


  async getUploadSignedUrl(uploadFilePathName: string, config?: { contentType?: string, selectedBucket?: string, expiry?: string}): Promise<string> {
    console.log("StorageService: getUploadSignedUrl:", uploadFilePathName)
    const expiryTime = config?.expiry || Math.floor(Date.now() / 1000) + URL_EXPIRATION_SECONDS;
    const contentType = config?.contentType || 'application/octet-stream';
    const payload = `${uploadFilePathName}:${expiryTime}:${contentType}`;
    const signature = generateSignature(payload, process.env.FS_UPLOAD_SECRET_KEY ?? ''); // Init will throw if secret key is not set
  
    return `${process.env.API_PUBLIC_URL}/api/services/utils/upload?filename=${uploadFilePathName}&expiry=${expiryTime}&signature=${signature}&contentType=${contentType}`;
  }
  
  async getDownloadSignedUrl(filePathName: string, config?: { contentType?: string, selectedBucket?: string, expiry?: string}): Promise<string> {
    console.log("StorageService: getDownloadSignedUrl:", filePathName)
    const expiryTime = config?.expiry || Math.floor(Date.now() / 1000) + URL_EXPIRATION_SECONDS;
    const contentType = config?.contentType || 'application/octet-stream';
    const payload = `${filePathName}:${expiryTime}:${contentType}`;
    const signature = generateSignature(payload, process.env.FS_UPLOAD_SECRET_KEY ?? ''); // Init will throw if secret key is not set

    return `${process.env.API_PUBLIC_URL}/api/services/utils/download?filename=${filePathName}&expiry=${expiryTime}&signature=${signature}&contentType=${contentType}`;
  }

  async getFileMetadata(fileName: string): Promise<{ md5Hash: string; fileUrl: string }> {
    console.debug('StorageService: getFileMetadata:', fileName);
    const filePath = path.join(this.baseDirectory, fileName);
    var fileUrl = '';
    var md5Hash = '';
    try {
      const fileBuffer = fs.readFileSync(filePath);
      md5Hash = createHash('md5').update(fileBuffer).digest('hex');
    }
    catch (error) {
      console.error('Error reading file:', error);
    }
    return { md5Hash, fileUrl };
  }
}

/* On GAE/Prod, we shall rely on default app engine service account credentials.
 * Two changes needed: 1) add default service account to our uploads GCS Bucket
 * with create and view access. 2) add 'Service Account Token Creator' role to
 * the default app engine service account on the IAM page. We also need to
 * enable IAM related APIs on the project.
 */
export const storageService: StorageService = process.env.USE_FS_STORAGE === 'true'
  ? new FileSystemStorageService(process.env.FS_UPLOAD_PATH || '/mnt/shared/omnivore-files')
  : new GCSStorageService(process.env.GCS_UPLOAD_BUCKET || 'omnivore-files', process.env.GCS_UPLOAD_SA_KEY_FILE_PATH);

