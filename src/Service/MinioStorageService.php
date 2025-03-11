<?php

namespace IntelligentIntern\MinioStorageBundle\Service;

use App\Factory\LogServiceFactory;
use App\Service\VaultService;
use App\Contract\StorageServiceInterface;
use App\Contract\LogServiceInterface;
use League\Flysystem\FilesystemException;
use League\Flysystem\FilesystemOperator;
use League\Flysystem\Filesystem;
use League\Flysystem\AwsS3V3\AwsS3V3Adapter;
use Aws\S3\S3Client;
use Aws\Exception\AwsException;
use DateTime;
use RuntimeException;
use Symfony\Contracts\HttpClient\Exception\ClientExceptionInterface;
use Symfony\Contracts\HttpClient\Exception\DecodingExceptionInterface;
use Symfony\Contracts\HttpClient\Exception\RedirectionExceptionInterface;
use Symfony\Contracts\HttpClient\Exception\ServerExceptionInterface;
use Symfony\Contracts\HttpClient\Exception\TransportExceptionInterface;

class MinioStorageService implements StorageServiceInterface
{
    private LogServiceInterface $logger;
    private FilesystemOperator $storage;
    private S3Client $s3Client;
    private string $bucket;

    /**
     * @throws TransportExceptionInterface
     * @throws ServerExceptionInterface
     * @throws RedirectionExceptionInterface
     * @throws DecodingExceptionInterface
     * @throws ClientExceptionInterface
     */
    public function __construct(
        private readonly VaultService $vaultService,
        private readonly LogServiceFactory $logServiceFactory
    ) {
        $this->logger = $this->logServiceFactory->create();
        $minioConfig = $this->vaultService->fetchSecret('secret/data/data/minio');

        $minioUrl  = $minioConfig['url'] ?? throw new RuntimeException('_URL not found in Vault.');
        $username  = $minioConfig['username'] ?? throw new RuntimeException('_USERNAME not found in Vault.');
        $password  = $minioConfig['password'] ?? throw new RuntimeException('_PASSWORD not found in Vault.');
        $this->bucket = $minioConfig['bucket'] ?? 'default-bucket';

        // Create the S3 client. (Additional options such as certificates can be added if needed.)
        $this->s3Client = new S3Client([
            'version'                 => 'latest',
            'region'                  => 'us-east-1', // Dummy region for MinIO
            'endpoint'                => $minioUrl,
            'credentials'             => [
                'key'    => $username,
                'secret' => $password,
            ],
            'use_path_style_endpoint' => true, // Important for MinIO
            // 'verify'              => '/path/to/certificate.pem', // Uncomment if you need to add a certificate
        ]);

        // Create the Flysystem adapter and FilesystemOperator instance
        $adapter = new AwsS3V3Adapter($this->s3Client, $this->bucket);
        $this->storage = new Filesystem($adapter);
    }

    /**
     * @param string $provider
     * @return bool
     */
    public function supports(string $provider): bool
    {
        return strtolower($provider) === 'minio';
    }

    /**
     * @param string $path
     * @return string|null
     */
    public function generatePresignedUrl(string $path): ?string
    {
        // Generate a temporary URL valid for 1 hour by default.
        return $this->storage->temporaryUrl($path, new DateTime('+1 hour'));
    }

    /**
     * @param string $path
     * @param int $expirySeconds
     * @return string|null
     */
    public function generateExpiringDownloadUrl(string $path, int $expirySeconds): ?string
    {
        $expiry = new DateTime("+{$expirySeconds} seconds");
        return $this->storage->temporaryUrl($path, $expiry);
    }

    /**
     * @param string $path
     * @param int $expirySeconds
     * @return string|null
     */
    public function generateExpiringUploadUrl(string $path, int $expirySeconds): ?string
    {
        $expiry = "+{$expirySeconds} seconds";
        try {
            $command = $this->s3Client->getCommand('PutObject', [
                'Bucket' => $this->bucket,
                'Key'    => $path,
            ]);
            $request = $this->s3Client->createPresignedRequest($command, $expiry);
            return (string)$request->getUri();
        } catch (AwsException $e) {
            $this->logger->error('Error generating expiring upload URL: ' . $e->getMessage());
            return null;
        }
    }

    /**
     * @throws FilesystemException
     */
    public function uploadFile(string $path, string $content): void
    {
        $this->storage->write($path, $content);
    }

    /**
     * @throws FilesystemException
     */
    public function deleteFile(string $path): void
    {
        $this->storage->delete($path);
    }

    /**
     * @param string $path
     * @return bool
     * @throws FilesystemException
     */
    public function fileExists(string $path): bool
    {
        return $this->storage->fileExists($path);
    }

    /**
     * @param string $path
     * @return string|null
     */
    public function getFileContent(string $path): ?string
    {
        try {
            return $this->storage->read($path);
        } catch (FilesystemException $e) {
            $this->logger->error("Error reading file [$path]: " . $e->getMessage());
            return null;
        }
    }

    /**
     * @param string $directory
     * @return array
     * @throws FilesystemException
     */
    public function listFiles(string $directory): array
    {
        $files = [];
        $listing = $this->storage->listContents($directory, false);
        foreach ($listing as $item) {
            // Depending on your Flysystem version, adjust accordingly.
            if (isset($item['type']) && $item['type'] === 'file') {
                $files[] = $item['path'];
            }
        }
        return $files;
    }

    /**
     * @param string $sourcePath
     * @param string $destinationPath
     * @return void
     * @throws FilesystemException
     */
    public function copyFile(string $sourcePath, string $destinationPath): void
    {
        $this->storage->copy($sourcePath, $destinationPath);
    }

    /**
     * @param string $sourcePath
     * @param string $destinationPath
     * @return void
     * @throws FilesystemException
     */
    public function moveFile(string $sourcePath, string $destinationPath): void
    {
        $this->storage->move($sourcePath, $destinationPath);
    }

    /**
     * @param string $path
     * @return array
     */
    public function getFileMetadata(string $path): array
    {
        try {
            return [
                'mimeType'     => $this->storage->mimeType($path),
                'lastModified' => $this->storage->lastModified($path),
                'fileSize'     => $this->storage->fileSize($path),
                'visibility'   => $this->storage->visibility($path),
            ];
        } catch (FilesystemException $e) {
            $this->logger->error("Error getting metadata for [$path]: " . $e->getMessage());
            return [];
        }
    }

    /**
     * @param string $path
     * @param array $metadata
     * @return void
     */
    public function setFileMetadata(string $path, array $metadata): void
    {
        // Flysystem does not directly support updating metadata.
        // Using S3Client to copy the object onto itself with new metadata.
        try {
            $existingContent = $this->storage->read($path);
            // Copying the object with updated metadata (this may create a new version if versioning is enabled)
            $this->s3Client->putObject([
                'Bucket'   => $this->bucket,
                'Key'      => $path,
                'Body'     => $existingContent,
                'Metadata' => $metadata,
            ]);
        } catch (FilesystemException | AwsException $e) {
            $this->logger->error("Error setting metadata for [$path]: " . $e->getMessage());
        }
    }

    /**
     * @param string $path
     * @param array $tags
     * @return void
     */
    public function tagFile(string $path, array $tags): void
    {
        $tagSet = [];
        foreach ($tags as $key => $value) {
            $tagSet[] = ['Key' => $key, 'Value' => $value];
        }
        try {
            $this->s3Client->putObjectTagging([
                'Bucket'  => $this->bucket,
                'Key'     => $path,
                'Tagging' => ['TagSet' => $tagSet],
            ]);
        } catch (AwsException $e) {
            $this->logger->error("Error tagging file [$path]: " . $e->getMessage());
        }
    }

    /**
     * @param string $path
     * @return array
     */
    public function getFileTags(string $path): array
    {
        try {
            $result = $this->s3Client->getObjectTagging([
                'Bucket' => $this->bucket,
                'Key'    => $path,
            ]);
            $tags = [];
            foreach ($result['TagSet'] as $tag) {
                $tags[$tag['Key']] = $tag['Value'];
            }
            return $tags;
        } catch (AwsException $e) {
            $this->logger->error("Error retrieving tags for [$path]: " . $e->getMessage());
            return [];
        }
    }

    /**
     * @param string $path
     * @param string $visibility
     * @return void
     * @throws FilesystemException
     */
    public function setFileVisibility(string $path, string $visibility): void
    {
        // Flysystem supports visibility updates.
        $this->storage->setVisibility($path, $visibility);
    }

    /**
     * @throws FilesystemException
     */
    public function getFileVisibility(string $path): string
    {
        return $this->storage->visibility($path);
    }

    /**
     * @throws FilesystemException
     */
    public function streamFile(string $path)
    {
        // Returns a stream resource
        return $this->storage->readStream($path);
    }

    /**
     * @throws FilesystemException
     */
    public function createDirectory(string $path): void
    {
        // S3 is object storage, but Flysystem simulates directories.
        $this->storage->createDirectory($path);
    }

    /**
     * @throws FilesystemException
     */
    public function deleteDirectory(string $path): void
    {
        $this->storage->deleteDirectory($path);
    }

    /**
     * @throws FilesystemException
     */
    public function getFileChecksum(string $path, string $algorithm = 'sha256'): string
    {
        $content = $this->storage->read($path);
        return hash($algorithm, $content);
    }

    /**
     * @throws FilesystemException
     */
    public function validateFileChecksum(string $path, string $expectedHash, string $algorithm = 'sha256'): bool
    {
        $computed = $this->getFileChecksum($path, $algorithm);
        return $computed === $expectedHash;
    }

    /**
     * @param string $path
     * @return array
     */
    public function getFileVersions(string $path): array
    {
        try {
            $result = $this->s3Client->listObjectVersions([
                'Bucket' => $this->bucket,
                'Prefix' => $path,
            ]);
            $versions = [];
            if (isset($result['Versions'])) {
                foreach ($result['Versions'] as $version) {
                    if ($version['Key'] === $path) {
                        $versions[] = $version;
                    }
                }
            }
            return $versions;
        } catch (AwsException $e) {
            $this->logger->error("Error getting versions for [$path]: " . $e->getMessage());
            return [];
        }
    }

    /**
     * @param string $path
     * @param string $versionId
     * @return void
     */
    public function restoreFileVersion(string $path, string $versionId): void
    {
        try {
            $this->s3Client->copyObject([
                'Bucket'     => $this->bucket,
                'CopySource' => "{$this->bucket}/{$path}?versionId={$versionId}",
                'Key'        => $path,
            ]);
        } catch (AwsException $e) {
            $this->logger->error("Error restoring version [$versionId] for [$path]: " . $e->getMessage());
        }
    }

    /**
     * @throws FilesystemException
     */
    public function encryptAndUploadFile(string $path, string $content, string $encryptionKey): void
    {
        $cipher = 'AES-256-CBC';
        $ivLength = openssl_cipher_iv_length($cipher);
        $iv = openssl_random_pseudo_bytes($ivLength);
        $encrypted = openssl_encrypt($content, $cipher, $encryptionKey, 0, $iv);
        // Prepend the IV so it can be used in decryption
        $dataToStore = base64_encode($iv . $encrypted);
        $this->uploadFile($path, $dataToStore);
    }

    /**
     * @param string $path
     * @param string $encryptionKey
     * @return string|null
     */
    public function decryptFile(string $path, string $encryptionKey): ?string
    {
        $data = $this->getFileContent($path);
        if ($data === null) {
            return null;
        }
        $data = base64_decode($data);
        $cipher = 'AES-256-CBC';
        $ivLength = openssl_cipher_iv_length($cipher);
        $iv = substr($data, 0, $ivLength);
        $encrypted = substr($data, $ivLength);
        return openssl_decrypt($encrypted, $cipher, $encryptionKey, 0, $iv);
    }

    /**
     * @throws FilesystemException
     */
    public function compressAndUploadFile(string $path, string $content, string $compressionType = 'gzip'): void
    {
        if ($compressionType === 'gzip') {
            $compressed = gzcompress($content);
            $this->uploadFile($path, $compressed);
        } else {
            // Other compression types can be implemented as needed.
            throw new RuntimeException("Compression type [$compressionType] not supported.");
        }
    }

    /**
     * @param string $path
     * @param string $compressionType
     * @return string|null
     */
    public function decompressFile(string $path, string $compressionType = 'gzip'): ?string
    {
        $compressed = $this->getFileContent($path);
        if ($compressed === null) {
            return null;
        }
        if ($compressionType === 'gzip') {
            return gzuncompress($compressed);
        } else {
            throw new RuntimeException("Compression type [$compressionType] not supported.");
        }
    }

    /**
     * @param string $path
     * @return string
     */
    public function multipartUploadInit(string $path): string
    {
        try {
            $result = $this->s3Client->createMultipartUpload([
                'Bucket' => $this->bucket,
                'Key'    => $path,
            ]);
            return $result['UploadId'];
        } catch (AwsException $e) {
            $this->logger->error("Error initiating multipart upload for [$path]: " . $e->getMessage());
            throw new RuntimeException("Multipart upload initiation failed.");
        }
    }

    /**
     * @param string $uploadId
     * @param string $path
     * @param int $partNumber
     * @param string $data
     * @return void
     */
    public function multipartUploadPart(string $uploadId, string $path, int $partNumber, string $data): void
    {
        try {
            $this->s3Client->uploadPart([
                'Bucket'     => $this->bucket,
                'Key'        => $path,
                'UploadId'   => $uploadId,
                'PartNumber' => $partNumber,
                'Body'       => $data,
            ]);
        } catch (AwsException $e) {
            $this->logger->error("Error uploading part [$partNumber] for [$path]: " . $e->getMessage());
            throw new RuntimeException("Multipart upload part failed.");
        }
    }

    /**
     * @param string $uploadId
     * @param string $path
     * @return void
     */
    public function multipartUploadComplete(string $uploadId, string $path): void
    {
        // NOTE: In a production environment, you should store each part's ETag from multipartUploadPart.
        // For simplicity, we list the parts here (which may not be fully reliable if the number of parts is large).
        try {
            $result = $this->s3Client->listParts([
                'Bucket'   => $this->bucket,
                'Key'      => $path,
                'UploadId' => $uploadId,
            ]);
            $parts = [];
            if (isset($result['Parts'])) {
                foreach ($result['Parts'] as $part) {
                    $parts[] = [
                        'ETag'       => $part['ETag'],
                        'PartNumber' => $part['PartNumber'],
                    ];
                }
            }
            $this->s3Client->completeMultipartUpload([
                'Bucket'          => $this->bucket,
                'Key'             => $path,
                'UploadId'        => $uploadId,
                'MultipartUpload' => ['Parts' => $parts],
            ]);
        } catch (AwsException $e) {
            $this->logger->error("Error completing multipart upload for [$path]: " . $e->getMessage());
            throw new RuntimeException("Multipart upload completion failed.");
        }
    }

    /**
     * @param string $path
     * @return array
     */
    public function getFileAccessLogs(string $path): array
    {
        // S3/MinIO does not provide per-object access logs by default.
        // This would normally be implemented via CloudTrail or another logging service.
        return [];
    }

    /**
     * @return array
     */
    public function getStorageUsage(): array
    {
        try {
            $result = $this->s3Client->listObjectsV2([
                'Bucket' => $this->bucket,
            ]);
            $totalSize = 0;
            $objectCount = 0;
            if (isset($result['Contents'])) {
                foreach ($result['Contents'] as $object) {
                    $totalSize += $object['Size'];
                    $objectCount++;
                }
            }
            return [
                'total_size'   => $totalSize,
                'object_count' => $objectCount,
            ];
        } catch (AwsException $e) {
            $this->logger->error("Error getting storage usage: " . $e->getMessage());
            return [];
        }
    }
}
