# Intelligent Intern MinIO Storage Bundle

The `intelligent-intern/minio-storage-bundle` integrates MinIO with the [Intelligent Intern Core Framework](https://github.com/Intelligent-Intern/core), enabling seamless object storage functionality with full Vault integration and AMQP event handling.

## Installation

Install the bundle using Composer:

~~~bash
composer require intelligent-intern/minio-storage-bundle
~~~

## Configuration

Ensure the following secrets are set in Vault:

~~~env
MINIO_URL=your_minio_url
MINIO_ACCESS_KEY=your_minio_access_key
MINIO_SECRET_KEY=your_minio_secret_key
MINIO_BUCKET=your_default_bucket
MINIO_CODE_BUCKET=your_code_bucket
MINIO_INCOMING_BUCKET=your_incoming_bucket
MINIO_LOGS_BUCKET=your_logs_bucket
~~~

Additionally, ensure AMQP settings are configured in MinIO for event notifications.

## Usage

Once the bundle is installed and configured, the Core framework will dynamically detect the MinIO service via the `storage.strategy` tag.

The service will be available via the `MinioService`:

~~~php
<?php

namespace App\Controller;

use IntelligentIntern\MinioStorageBundle\Service\MinioStorageService;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;

class StorageController extends AbstractController
{
    public function __construct(
        private MinioStorageService $minioService
    ) {}

    public function uploadFile(Request $request): JsonResponse
    {
        $bucket = $request->get('bucket', 'default');
        $path = $request->get('path');
        $content = $request->get('content');

        if (empty($path) || empty($content)) {
            return new JsonResponse(['error' => 'Path and content are required'], 400);
        }

        try {
            $this->minioService->uploadFile($bucket, $path, $content);
            return new JsonResponse(['message' => 'File uploaded successfully']);
        } catch (\Exception $e) {
            return new JsonResponse(['error' => $e->getMessage()], 500);
        }
    }

    public function generatePresignedUrl(Request $request): JsonResponse
    {
        $bucket = $request->get('bucket', 'default');
        $path = $request->get('path');

        if (empty($path)) {
            return new JsonResponse(['error' => 'Path is required'], 400);
        }

        try {
            $url = $this->minioService->generatePresignedUrl($bucket, $path);
            return new JsonResponse(['url' => $url]);
        } catch (\Exception $e) {
            return new JsonResponse(['error' => $e->getMessage()], 500);
        }
    }
}
~~~

## Event-Driven Architecture

This bundle supports **event-based storage** using **AMQP (RabbitMQ)**.  
MinIO is configured to send **file change events** to AMQP, enabling asynchronous processing.

To set up an event listener:

~~~php
<?php

namespace App\Service;

use IntelligentIntern\MinioStorageBundle\Service\MinioStorageService;

class StorageEventService
{
    public function __construct(private MinioStorageService $minioService) {}

    public function setupBucketEvents(): void
    {
        $this->minioService->setBucketEventListener('incoming', 'incoming_queue');
        $this->minioService->setBucketEventListener('code', 'code_queue');
    }
}
~~~

## Features

- Multi-bucket support (`incoming`, `logs`, `code`)
- Vault integration (fetches MinIO credentials dynamically)
- AMQP event publishing (triggers events on file operations)
- Expiring download/upload URLs
- File metadata and tagging
- File encryption & decryption
- Multipart uploads for large files
- GraphDB integration-ready (for context-aware file retrieval)

## Extensibility

This bundle is designed to integrate with `intelligent-intern/core` and can be extended further.  
To implement **custom storage strategies**, create a new bundle implementing `MinioServiceInterface` and tag the service with `storage.strategy`.

For contributions, reach out to `jschultz@php.net` for guidelines.

## License

This bundle is open-sourced software licensed under the [MIT license](LICENSE).
