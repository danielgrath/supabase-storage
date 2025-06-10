# Google Cloud Storage Configuration

This document explains how to configure Supabase Storage to use Google Cloud Storage (GCS) as the backend.

## Configuration Overview

GCS backend reuses most of the S3 configuration options to minimize configuration overhead. Only authentication-specific settings are different.

## Required Environment Variables

### Shared Configuration (reused from S3)
```bash
# Storage backend type
STORAGE_BACKEND=gcs

# Bucket name (same as S3)
STORAGE_S3_BUCKET=my-storage-bucket

# Region (same as S3, but use GCS region names)
STORAGE_S3_REGION=us-central1

# Connection settings (same as S3)
STORAGE_S3_MAX_SOCKETS=200
STORAGE_S3_CLIENT_TIMEOUT=0
```

### GCS-Specific Authentication
```bash
# Option 1: Using service account key file
STORAGE_GCS_KEY_FILE_PATH=/path/to/service-account-key.json

# Option 2: Using service account credentials as JSON string
STORAGE_GCS_CREDENTIALS='{"type":"service_account","project_id":"my-project",...}'

# Option 3: Using environment variables (fallback)
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
GOOGLE_CLOUD_PROJECT=my-project-id

# Explicit project ID (optional, can be inferred from credentials)
STORAGE_GCS_PROJECT_ID=my-project-id
```

## Configuration Comparison

### S3 Configuration
```bash
STORAGE_BACKEND=s3
STORAGE_S3_BUCKET=my-bucket
STORAGE_S3_REGION=us-east-1
STORAGE_S3_ENDPOINT=https://s3.amazonaws.com
STORAGE_S3_FORCE_PATH_STYLE=false
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
```

### GCS Configuration
```bash
STORAGE_BACKEND=gcs
STORAGE_S3_BUCKET=my-bucket                    # ✅ Reused
STORAGE_S3_REGION=us-central1                  # ✅ Reused (GCS region name)
# STORAGE_S3_ENDPOINT - not needed for GCS     # ❌ Not used
# STORAGE_S3_FORCE_PATH_STYLE - not needed     # ❌ Not used
STORAGE_GCS_KEY_FILE_PATH=/path/to/key.json    # ✅ GCS-specific
```

## Authentication Methods

### 1. Service Account Key File
The most straightforward method:
```bash
STORAGE_GCS_KEY_FILE_PATH=/path/to/service-account-key.json
```

### 2. Inline Credentials
For containerized environments:
```bash
STORAGE_GCS_CREDENTIALS='{"type":"service_account","project_id":"my-project","private_key_id":"...","private_key":"-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n","client_email":"storage@my-project.iam.gserviceaccount.com","client_id":"...","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs","client_x509_cert_url":"https://www.googleapis.com/robot/v1/metadata/x509/storage%40my-project.iam.gserviceaccount.com"}'
```

### 3. Default Application Credentials
For Google Cloud environments (GKE, Cloud Run, etc.):
```bash
# No explicit credentials needed - uses instance service account
STORAGE_BACKEND=gcs
STORAGE_S3_BUCKET=my-bucket
STORAGE_S3_REGION=us-central1
```

## Required Permissions

Your service account needs the following IAM roles:
- `Storage Object Admin` on the bucket
- `Storage Legacy Bucket Writer` (if using legacy bucket permissions)

Or these specific permissions:
- `storage.objects.create`
- `storage.objects.delete` 
- `storage.objects.get`
- `storage.objects.list`
- `storage.objects.update`

## TUS Upload Support

**Note**: TUS (resumable uploads) support for GCS is not yet implemented. When using GCS backend:
- Regular uploads work fully with GCS
- TUS uploads fall back to local file storage
- This is a known limitation and will be addressed in future versions

## Migration from S3 to GCS

1. Change `STORAGE_BACKEND` from `s3` to `gcs`
2. Keep `STORAGE_S3_BUCKET` and `STORAGE_S3_REGION` (update region to GCS format)
3. Add GCS authentication (`STORAGE_GCS_KEY_FILE_PATH` or `STORAGE_GCS_CREDENTIALS`)
4. Remove S3-specific configs (`STORAGE_S3_ENDPOINT`, `STORAGE_S3_FORCE_PATH_STYLE`)

The migration is minimal because bucket name, region, and connection settings are reused! 