# Chatbot document ingestion

## Source and schedule

- **Initial source:** the `sf-chatbot-documents` S3 bucket. The daily Airflow DAG reconciles its configured prefix.
- Microsoft SharePoint remains the planned upstream document system. Once permissions are available, its ingestion process should upload new or changed files to this bucket.
- SharePoint webhooks or Microsoft Graph change notifications can later trigger prompt processing. Keep the daily S3 DAG as a reconciliation fallback.

## Ingestion flow

1. Enumerate the configured S3 prefix and identify each document by bucket and key.
2. Compare its VersionId when available, otherwise its ETag; retain `LastModified` for audit and fallback comparison. Skip unchanged indexed documents.
3. Chunk each new or changed document, generate Titan Text Embeddings V2 vectors, and write them to PostgreSQL pgvector.
4. Prepare all replacement chunks and embeddings before replacing the document's rows in `chatbot.embeddings`.
5. Mark the `chatbot.ingestion_state` row `indexed` only after the replacement succeeds. Failed or incomplete files must be retried.
6. Detect objects removed from S3 and remove or deactivate their embeddings.

## Document tracking

Create `chatbot.ingestion_state`, keyed by the document key. Track the S3 bucket/key, ETag, optional VersionId, last-modified time, processing status, last successful ingestion time, and any error details. Upsert the ingestion state as the DAG processes each item; only advance the indexed version after successful embedding and vector writes. Store the vectors and chunk metadata in `chatbot.embeddings`.

## S3 change detection

Do not hash document contents for routine reconciliation. Store each S3 object's key, ETag, optional VersionId, `LastModified` value, and successful indexing timestamp in `chatbot.ingestion_state`. Index a document when its key is new or its ETag or VersionId differs from the indexed record. Use `LastModified` as a fallback comparison and audit value. On change, replace every existing chunk and vector in `chatbot.embeddings` for that document with a complete newly chunked and embedded version.

Keep the S3 bucket/prefix, embedding model, chunk size/overlap, and DAG schedule configurable. Do not store AWS or future SharePoint credentials in DAG code; use the configured secrets mechanism.

References for the future SharePoint upstream: [SharePoint list webhooks](https://learn.microsoft.com/en-us/sharepoint/dev/apis/webhooks/lists/overview-sharepoint-list-webhooks) · [Microsoft Graph change notifications](https://learn.microsoft.com/en-us/graph/change-notifications-overview) · [Webhook delivery requirements](https://learn.microsoft.com/en-us/graph/change-notifications-delivery-webhooks)

## PostgreSQL chatbot schema

Store chatbot ingestion metadata in `chatbot.ingestion_state` and vector records in `chatbot.embeddings` in the existing PostgreSQL database. `ingestion_state` holds processing flags such as pending, processing, indexed, and failed; `embeddings` holds each chunk and its Bedrock vector. Keep the S3 document objects in `sf-chatbot-documents`; the schema tracks each source file and its indexing state.
