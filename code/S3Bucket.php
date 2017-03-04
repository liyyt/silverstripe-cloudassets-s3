<?php
/**
 * Bucket/container driver for AWS S3
 * Based on:
 * - https://github.com/markguinn/silverstripe-cloudassets-rackspace
 * - https://github.com/edlinklater/silverstripe-cloudassets-s3
 *
 * @author Aaron Bolanos <aaron@liyyt.com>
 * @package cloudassets
 * @subpackage buckets
 */

use Aws\S3\S3Client;

class S3Bucket extends CloudBucket
{

    const BUCKET = 'Bucket';
    const REGION = 'Region';
    const API_KEY = 'ApiKey';
    const API_SECRET = 'ApiSecret';

    private $client;
    private $bucket;
    private $apiKey;
    private $apiSecret;

    public function __construct($path, array $cfg = array()) {
        parent::__construct($path, $cfg);

        if (empty(getenv('S3_BUCKET')) && empty($cfg[self::BUCKET]))
            throw new Exception('S3Bucket: missing configuration key - '.self::BUCKET);

        if (empty(getenv('S3_REGION')) && empty($cfg[self::REGION]))
            throw new Exception('S3Bucket: missing configuration key - '.self::REGION);

        if (empty(getenv('S3_API_KEY')) && empty($cfg[self::API_KEY]))
            throw new Exception('S3Bucket: missing configuration key - '.self::API_KEY);

        if (empty(getenv('S3_API_SECRET')) && empty($cfg[self::API_SECRET]))
            throw new Exception('S3Bucket: missing configuration key - '.self::API_SECRET);

        $region = getenv('S3_REGION') ?: $cfg[self::REGION];

        $this->container = getenv('S3_BUCKET') ?: $cfg[self::BUCKET];
        $this->apiKey = getenv('S3_API_KEY') ?: $cfg[self::API_KEY];
        $this->apiSecret = getenv('S3_API_SECRET') ?: $cfg[self::API_SECRET];

        $this->client = new S3Client([
            'version' => 'latest',
            'region'  => $region
        ]);
    }

    public function put(File $f) {
        try {
            $this->client->putObject([
               'Bucket' => $this->bucket,
               'Key' => $this->getRelativeLinkFor($f),
               'Body' => fopen($f->getFullPath(), 'r'),
               'ACL' => 'public-read'
           ]);
        } catch (Aws\S3\Exception\S3Exception $e) {}
    }

    public function delete($f) {
        $this->client->deleteObject([
            'Bucket' => $this->bucket,
            'Key' => $this->getRelativeLinkFor($f)
        ]);
    }

    public function rename(File $f, $beforeName, $afterName) {
        $result = $this->client->copyObject([
            'Bucket' => $this->bucket,
            'CopySource' => urlencode($this->bucket.'/'.$this->getRelativeLinkFor($beforeName)),
            'Key' => $this->getRelativeLinkFor($afterName)
        ]);

        if($result) $this->delete($beforeName);
    }

    public function getContents(File $f) {
        try {
            $result = $this->client->getObject([
                'Bucket' => $this->bucket,
                'Key'    => $this->getRelativeLinkFor($f)
            ]);
            return $result['Body'];
        } catch (\Aws\S3\Exception\NoSuchKeyException $e) {
            return -1;
        }
    }
}