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
    private string $region;
    private string $version;
    private bool $usePathStyleEndpoint;
    private bool|string $verify;
    private string $endpoint;
    private string $username;
    private string $password;

    /**
     * @param VaultService $vaultService
     * @param LogServiceFactory $logServiceFactory
     * @throws ClientExceptionInterface
     * @throws DecodingExceptionInterface
     * @throws RedirectionExceptionInterface
     * @throws ServerExceptionInterface
     * @throws TransportExceptionInterface
     */
    public function __construct(
        private readonly VaultService $vaultService,
        private readonly LogServiceFactory $logServiceFactory
    ) {
        $this->logger = $this->logServiceFactory->create();
        $minioConfig = $this->vaultService->fetchSecret('secret/data/minio');

        $this->endpoint = $minioConfig['minio_endpoint']
            ?? throw new RuntimeException('MinIO endpoint not found in Vault.');
        $this->username = $minioConfig['minio_access_key']
            ?? throw new RuntimeException('MinIO access key not found in Vault.');
        $this->password = $minioConfig['minio_secret_key']
            ?? throw new RuntimeException('MinIO secret key not found in Vault.');
        $this->bucket   = $minioConfig['incoming_bucket']
            ?? 'default-incoming-bucket';
        $this->region   = $minioConfig['region']
            ?? 'us-east-1';
        $this->version  = $minioConfig['version']
            ?? 'latest';
        $this->usePathStyleEndpoint = !isset($minioConfig['minio_use_local_log_storage'])
            || $minioConfig['minio_use_local_log_storage'];
        $this->verify = $minioConfig['verify']
            ?? false;

        $this->initializeClient();
    }

    private function initializeClient(): void
    {
        $this->s3Client = new S3Client([
            'version'                 => $this->version,
            'region'                  => $this->region,
            'endpoint'                => $this->endpoint,
            'credentials'             => [
                'key'    => $this->username,
                'secret' => $this->password,
            ],
            'use_path_style_endpoint' => $this->usePathStyleEndpoint,
            'verify'                  => $this->verify,
        ]);
        $adapter = new AwsS3V3Adapter($this->s3Client, $this->bucket);
        $this->storage = new Filesystem($adapter);
    }

    private function updateS3Client(): void
    {
        $this->initializeClient();
    }

    /**
     * @inheritDoc
     */
    public function supports(string $provider): bool
    {
        return strtolower($provider) === 'minio';
    }

    /**
     * @inheritDoc
     */
    public function setRegion(string $region): void
    {
        $this->region = $region;
        $this->updateS3Client();
    }

    /**
     * @inheritDoc
     */
    public function getRegion(): string
    {
        return $this->region;
    }

    /**
     * @inheritDoc
     */
    public function setVersion(string $version): void
    {
        $this->version = $version;
        $this->updateS3Client();
    }

    /**
     * @inheritDoc
     */
    public function getVersion(): string
    {
        return $this->version;
    }

    /**
     * @inheritDoc
     */
    public function setUsePathStyleEndpoint(bool $usePathStyleEndpoint): void
    {
        $this->usePathStyleEndpoint = $usePathStyleEndpoint;
        $this->updateS3Client();
    }

    /**
     * @inheritDoc
     */
    public function getUsePathStyleEndpoint(): bool
    {
        return $this->usePathStyleEndpoint;
    }

    /**
     * @inheritDoc
     */
    public function setVerify(bool|string $verify): void
    {
        $this->verify = $verify;
        $this->updateS3Client();
    }

    /**
     * @inheritDoc
     */
    public function getVerify(): bool|string
    {
        return $this->verify;
    }

    /**
     * @inheritDoc
     */
    public function createBucket(string $bucket): void
    {
        try {
            $this->s3Client->createBucket(['Bucket' => $bucket]);
        } catch (AwsException $e) {
            throw new RuntimeException("Bucket creation failed: " . $e->getMessage());
        }
    }

    /**
     * @inheritDoc
     */
    public function deleteBucket(string $bucket): void
    {
        try {
            $this->s3Client->deleteBucket(['Bucket' => $bucket]);
        } catch (AwsException $e) {
            throw new RuntimeException("Bucket deletion failed: " . $e->getMessage());
        }
    }

    /**
     * @inheritDoc
     */
    public function setBucketEvents(string $bucket, array $events): void
    {
        try {
            $this->s3Client->putBucketNotificationConfiguration([
                'Bucket'                    => $bucket,
                'NotificationConfiguration' => $events,
            ]);
        } catch (AwsException $e) {
            throw new RuntimeException("Setting bucket events failed: " . $e->getMessage());
        }
    }

    /**
     * @inheritDoc
     */
    public function getStorageUsage(): array
    {
        try {
            $result = $this->s3Client->listObjectsV2(['Bucket' => $this->bucket]);
            $totalSize = array_sum(array_column($result['Contents'] ?? [], 'Size'));
            $objectCount = count($result['Contents'] ?? []);
            return ['total_size' => $totalSize, 'object_count' => $objectCount];
        } catch (AwsException) {
            return [];
        }
    }

    /**
     * @inheritDoc
     * @throws FilesystemException
     */
    public function encryptAndUploadFile(string $path, string $content, string $encryptionKey): void
    {
        $cipher = 'AES-256-CBC';
        $iv = random_bytes(openssl_cipher_iv_length($cipher));
        $encrypted = openssl_encrypt($content, $cipher, $encryptionKey, 0, $iv);
        $this->uploadFile($path, base64_encode($iv . $encrypted));
    }

    /**
     * @inheritDoc
     */
    public function decryptFile(string $path, string $encryptionKey): ?string
    {
        $data = base64_decode($this->getFileContent($path) ?? '');
        $cipher = 'AES-256-CBC';
        $ivLength = openssl_cipher_iv_length($cipher);
        return openssl_decrypt(substr($data, $ivLength), $cipher, $encryptionKey, 0, substr($data, 0, $ivLength));
    }

    /**
     * @inheritDoc
     * @throws FilesystemException
     */
    public function uploadFile(string $path, string $content): void
    {
        $this->storage->write($path, $content);
    }

    /**
     * @inheritDoc
     * @throws FilesystemException
     */
    public function deleteFile(string $path): void
    {
        $this->storage->delete($path);
    }

    /**
     * @inheritDoc
     * @throws FilesystemException
     */
    public function fileExists(string $path): bool
    {
        return $this->storage->fileExists($path);
    }

    /**
     * @inheritDoc
     */
    public function getFileContent(string $path): ?string
    {
        try {
            return $this->storage->read($path);
        } catch (FilesystemException) {
            return null;
        }
    }

    /**
     * @inheritDoc
     * @throws FilesystemException
     */
    public function listFiles(string $directory): array
    {
        return array_map(fn($item) => $item['path'], array_filter(
            (array)$this->storage->listContents($directory, false),
            fn($item) => isset($item['type']) && $item['type'] === 'file'
        ));
    }

    /**
     * @inheritDoc
     * @throws FilesystemException
     */
    public function copyFile(string $sourcePath, string $destinationPath): void
    {
        $this->storage->copy($sourcePath, $destinationPath);
    }

    /**
     * @inheritDoc
     * @throws FilesystemException
     */
    public function moveFile(string $sourcePath, string $destinationPath): void
    {
        $this->storage->move($sourcePath, $destinationPath);
    }

    /**
     * @inheritDoc
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
            return [];
        }
    }

    /**
     * @inheritDoc
     */
    public function setFileMetadata(string $path, array $metadata): void
    {
        try {
            $existingContent = $this->storage->read($path);
            $this->s3Client->putObject([
                'Bucket'   => $this->bucket,
                'Key'      => $path,
                'Body'     => $existingContent,
                'Metadata' => $metadata,
            ]);
        } catch (FilesystemException | AwsException $e) {
            throw new RuntimeException("Error setting metadata: " . $e->getMessage());
        }
    }

    /**
     * @inheritDoc
     * @throws FilesystemException
     */
    public function setFileVisibility(string $path, string $visibility): void
    {
        $this->storage->setVisibility($path, $visibility);
    }

    /**
     * @inheritDoc
     * @throws FilesystemException
     */
    public function getFileVisibility(string $path): string
    {
        return $this->storage->visibility($path);
    }

    /**
     * @inheritDoc
     * @throws FilesystemException
     */
    public function streamFile(string $path)
    {
        return $this->storage->readStream($path);
    }

    /**
     * @inheritDoc
     * @throws FilesystemException
     */
    public function createDirectory(string $path): void
    {
        $this->storage->createDirectory($path);
    }

    /**
     * @inheritDoc
     * @throws FilesystemException
     */
    public function deleteDirectory(string $path): void
    {
        $this->storage->deleteDirectory($path);
    }

    /**
     * @inheritDoc
     * @throws FilesystemException
     */
    public function getFileChecksum(string $path, string $algorithm = 'sha256'): string
    {
        return hash($algorithm, $this->storage->read($path));
    }

    /**
     * @inheritDoc
     * @throws FilesystemException
     */
    public function validateFileChecksum(string $path, string $expectedHash, string $algorithm = 'sha256'): bool
    {
        return $this->getFileChecksum($path, $algorithm) === $expectedHash;
    }

    /**
     * @inheritDoc
     */
    public function getFileVersions(string $path): array
    {
        try {
            $result = $this->s3Client->listObjectVersions(['Bucket' => $this->bucket, 'Prefix' => $path]);
            return $result['Versions'] ?? [];
        } catch (AwsException $e) {
            return [];
        }
    }

    /**
     * @inheritDoc
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
            throw new RuntimeException("Error restoring file version: " . $e->getMessage());
        }
    }

    /**
     * @inheritDoc
     */
    public function generateExpiringDownloadUrl(string $path, int $expirySeconds): ?string
    {
        return $this->storage->temporaryUrl($path, new DateTime("+{$expirySeconds} seconds"));
    }

    /**
     * @inheritDoc
     */
    public function generateExpiringUploadUrl(string $path, int $expirySeconds): ?string
    {
        try {
            $command = $this->s3Client->getCommand('PutObject', ['Bucket' => $this->bucket, 'Key' => $path]);
            return (string) $this->s3Client->createPresignedRequest($command, "+{$expirySeconds} seconds")->getUri();
        } catch (AwsException $e) {
            return null;
        }
    }

    /**
     * @inheritDoc
     */
    public function getFileAccessLogs(string $path): array
    {
        return [];
    }

    /**
     * @inheritDoc
     */
    public function compressAndUploadFile(string $path, string $content, string $compressionType = 'gzip'): void
    {
        if ($compressionType === 'gzip') {
            $compressed = gzcompress($content);
            $this->uploadFile($path, $compressed);
        } else {
            throw new RuntimeException("Compression type [$compressionType] not supported.");
        }
    }

    /**
     * @inheritDoc
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
     * @inheritDoc
     */
    public function multipartUploadInit(string $path): string
    {
        try {
            $result = $this->s3Client->createMultipartUpload(['Bucket' => $this->bucket, 'Key' => $path]);
            return $result['UploadId'];
        } catch (AwsException $e) {
            throw new RuntimeException("Multipart upload initiation failed: " . $e->getMessage());
        }
    }

    /**
     * @inheritDoc
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
            throw new RuntimeException("Multipart upload part failed: " . $e->getMessage());
        }
    }

    /**
     * @inheritDoc
     */
    public function multipartUploadComplete(string $uploadId, string $path): void
    {
        try {
            $result = $this->s3Client->listParts([
                'Bucket'   => $this->bucket,
                'Key'      => $path,
                'UploadId' => $uploadId,
            ]);
            $parts = array_map(fn($part) => ['ETag' => $part['ETag'], 'PartNumber' => $part['PartNumber']], $result['Parts'] ?? []);
            $this->s3Client->completeMultipartUpload([
                'Bucket'          => $this->bucket,
                'Key'             => $path,
                'UploadId'        => $uploadId,
                'MultipartUpload' => ['Parts' => $parts],
            ]);
        } catch (AwsException $e) {
            throw new RuntimeException("Multipart upload completion failed: " . $e->getMessage());
        }
    }

    /**
     * @inheritDoc
     */
    public function tagFile(string $path, array $tags): void
    {
        $tagSet = array_map(fn($key, $value) => ['Key' => $key, 'Value' => $value], array_keys($tags), $tags);
        try {
            $this->s3Client->putObjectTagging([
                'Bucket'  => $this->bucket,
                'Key'     => $path,
                'Tagging' => ['TagSet' => $tagSet],
            ]);
        } catch (AwsException $e) {
            throw new RuntimeException("Error tagging file [$path]: " . $e->getMessage());
        }
    }

    /**
     * @inheritDoc
     */
    public function getFileTags(string $path): array
    {
        try {
            $result = $this->s3Client->getObjectTagging(['Bucket' => $this->bucket, 'Key' => $path]);
            return array_column($result['TagSet'] ?? [], 'Value', 'Key');
        } catch (AwsException $e) {
            return [];
        }
    }

    /**
     * @inheritDoc
     */
    public function generatePreSignedUrl(string $path): ?string
    {
        return $this->storage->temporaryUrl($path, new \DateTime('+1 hour'));
    }

}
