# Data contract

## Source Parquet schemas

| Convention | Contract |
| --- | --- |
| Storage | Flat, Snappy-compressed Parquet; columns in the exact order below |
| Nullability | Physically nullable for invalid-row fixtures; required fields must be non-null and required text non-blank |
| `string`, `bool` | UTF-8 `BYTE_ARRAY`, `BOOLEAN` |
| `int32`, `int64` | Signed `INT32`, signed `INT64` |
| `timestamp` | UTC microseconds: `INT64 TIMESTAMP(isAdjustedToUTC=true)` |
| `money` | EUR `DECIMAL(12,2)`, `FIXED_LEN_BYTE_ARRAY(6)` |

| Historical v1 entity | Columns in file order |
| --- | --- |
| Customers | `customer_id string`, `created_at timestamp`, `country string`, `customer_segment string` |
| Products | `product_id string`, `name string`, `category string`, `current_price money`, `is_active bool`, `updated_at timestamp`, `source_sequence int64` |
| Orders | `order_id string`, `customer_id string`, `order_date timestamp`, `status string`, `updated_at timestamp`, `source_sequence int64` |
| Order items | `item_id string`, `order_id string`, `product_id string`, `quantity int32`, `unit_price money`, `updated_at timestamp`, `source_sequence int64` |

| Incremental envelope position | Column | Type | Rule |
| --- | --- | --- | --- |
| 1 | `event_id` | string | Stable event identifier; redelivery may repeat it |
| 2 | `entity_type` | string | Matches the manifest entity |
| 3 | `record_id` | string | Equals the payload business key |
| 4 | `operation` | string | `insert` or `update`; full-row payload |
| 5 | `event_time` | timestamp | Business event time |
| 6 | `updated_at` | timestamp | Record version time |
| 7 | `source_sequence` | int64 | Non-negative version tie-breaker |
| 8 | `arrival_at` | timestamp | UTC date equals manifest `arrival_date` |

| Incremental v1 entity | Payload appended after the envelope, in order |
| --- | --- |
| Customers | `customer_id string`, `created_at timestamp`, `country string`, `customer_segment string` |
| Products | `product_id string`, `name string`, `category string`, `current_price money`, `is_active bool` |
| Orders | `order_id string`, `customer_id string`, `order_date timestamp`, `status string` |
| Order items | `item_id string`, `order_id string`, `product_id string`, `quantity int32`, `unit_price money` |

| Validation/version rule | Contract |
| --- | --- |
| Keys/references | Stable entity IDs; orders reference customers; items reference orders and products; accepted current rows are unique by key |
| Required values | All business fields required except v2 discount; country is two uppercase letters |
| Prices/quantity | Prices non-negative; quantity positive; source sequence non-negative |
| Order status | `pending`, `paid`, `shipped`, `delivered`, `canceled` |
| Schema v2 | Only order items; append `discount_amount money` to the corresponding v1 layout |
| Discount | Line-total discount between zero and `quantity * unit_price`; absent v1/null v2 normalize to zero |
| Loading | Validate declared layout before positional COPY; retain separate v1/v2 staging layouts |
| Version | Compare `(updated_at, source_sequence)` per key; historical customers start at `(created_at, 0)` |
| Invalid input | Quarantine invalid business rows; malformed manifests, layouts, event identity or conflicting accepted versions stop the delivery |

## Batch manifest

| Field | Type | Rule |
| --- | --- | --- |
| `dataset_id`, `batch_id` | string | Ledger key; lowercase `[a-z0-9][a-z0-9_-]*`; one dataset per core target |
| `kind` | string | `historical` or `incremental` |
| `arrival_date` | string | UTC `YYYY-MM-DD` |
| `files` | array | Non-empty; historical includes all four entities; incremental may omit unchanged entities |
| `files[].path` | string | Unique relative `.parquet` path; `/` separators; no absolute paths, empty components, `.` or `..` |
| `files[].entity_type` | string | `customers`, `products`, `orders`, `order_items` |
| `files[].schema_version` | integer | v1 for all entities; v2 only for items; one version per entity per delivery |
| `files[].row_count` | integer | Positive physical count before deduplication/quarantine |
| `files[].sha256` | string | 64 lowercase hexadecimal characters; exact file bytes |

| Delivery rule | Contract |
| --- | --- |
| Publication | `raw/<dataset_id>/<batch_id>/`; Parquet and `copy/<entity>.json` first; source `manifest.json` last |
| COPY manifests | Uploader-generated exact S3 URLs; each entry mandatory and includes content length |
| Integrity | Verify file checksums, physical schemas and row counts; ledger stores exact source-manifest SHA-256 |
| Release checksum | Paths relative to `data/<dataset_id>/`; sort case-sensitively with `/`; hash the UTF-8 sequence `<manifest-sha256>  <path>\n`, including the final LF |
| Selection | S3 source-manifest URI, or inclusive UTC cutoff; snapshot first, then normal daily batches by `(arrival_date, batch_id)` |
| Scenarios | Explicit URI; excluded from normal cutoff discovery |
| Relationships | Resolve against accepted delivery rows and committed core; customer/product before orders, then items |

## Metrics

| Metric/grain | Definition |
| --- | --- |
| Eligibility | Orders with status `paid`, `shipped` or `delivered`; EUR; UTC `order_date` |
| Net line/order | `quantity * unit_price - effective_discount`; order amount sums its lines, zero if none |
| Daily sales | One row per qualifying order date; sum net order amounts; dates with no eligible orders omitted |
| Orders/AOV | Count each eligible order once; total net sales / eligible order count, rounded to cents; dashboard shows zero when no eligible orders |
| Product performance | One row per current product; eligible net sales, units and distinct orders; zero for no sales |
| Product ranking/category | Rank by net sales, product ID breaks ties; categories use current product attribution |
| Customer activity | One row per customer; eligible order count, net sales, first/last eligible UTC dates |
| Repeat customer/rate | At least two eligible orders; repeat customers / customers with at least one eligible order; dashboard shows zero when no purchasing customers |
| Historical restatement | Late orders and corrected statuses, amounts or categories restate affected dates/category totals |
