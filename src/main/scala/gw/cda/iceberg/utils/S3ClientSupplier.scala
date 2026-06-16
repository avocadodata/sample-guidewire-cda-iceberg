// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

package gw.cda.iceberg.utils

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder

/** Single AmazonS3 client instance, used by the manifest reader.
  *
  * `withForceGlobalBucketAccessEnabled(true)` lets us read manifests from
  * source buckets in a different region than the one we're running in —
  * the CDA writer publishes once per region, but a customer's deployment
  * may live elsewhere. The standard credential chain finds the EMR job
  * role's credentials at runtime.
  */
object S3ClientSupplier {
  val s3Client: AmazonS3 =
    AmazonS3ClientBuilder.standard().withForceGlobalBucketAccessEnabled(true).build()
}
