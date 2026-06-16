// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

// Custom-resource handler for the optional AnalyticsStack: sets up the Glue
// federated catalog (parent + per-bucket child) for S3 Tables, and issues
// Lake Formation SELECT/DESCRIBE grants to analyst roles.
//
// Extracted from an inline `lambda.Code.fromInline(`...`)` block into this
// asset file (lambda.Code.fromAsset) so the source is real, syntax-checked
// JS — and so a static scanner doesn't misread the embedded template
// literals. The ${process.env.*} expressions here are normal runtime
// template strings (no escaping needed in a real file).
import { GlueClient, CreateCatalogCommand } from '@aws-sdk/client-glue';
import { LakeFormationClient, GrantPermissionsCommand } from '@aws-sdk/client-lakeformation';
import { isAlreadyExists } from './idempotency.mjs';

const glue = new GlueClient({});
const lf = new LakeFormationClient({});

async function ensureGlueFederation(parent, child, tableBucketArn) {
  // Parent (account-singleton). Try to create; ignore AlreadyExists.
  try {
    await glue.send(new CreateCatalogCommand({
      Name: parent,
      CatalogInput: {
        FederatedCatalog: {
          Identifier: `arn:aws:s3tables:${process.env.AWS_REGION}:${process.env.AWS_ACCOUNT_ID}:bucket/*`,
          ConnectionName: 'aws:s3tables',
        },
        CreateDatabaseDefaultPermissions: [],
        CreateTableDefaultPermissions: [],
      },
    }));
  } catch (e) {
    if (!isAlreadyExists(e)) throw e;
  }
  // Child (per-bucket). Same try-or-skip pattern.
  try {
    await glue.send(new CreateCatalogCommand({
      Name: child,
      CatalogInput: {
        FederatedCatalog: {
          Identifier: tableBucketArn,
          ConnectionName: 'aws:s3tables',
        },
        CreateDatabaseDefaultPermissions: [],
        CreateTableDefaultPermissions: [],
      },
    }));
  } catch (e) {
    if (!isAlreadyExists(e)) throw e;
  }
}

async function grantLf(catalogId, dbName, principalArn, perms) {
  const permissions = perms.split(',').map(p => p.trim());
  // Grant on the database (DESCRIBE) and on the table-wildcard (SELECT+DESCRIBE).
  await lf.send(new GrantPermissionsCommand({
    Principal: { DataLakePrincipalIdentifier: principalArn },
    Resource: { Database: { CatalogId: catalogId, Name: dbName } },
    Permissions: ['DESCRIBE'],
  }));
  await lf.send(new GrantPermissionsCommand({
    Principal: { DataLakePrincipalIdentifier: principalArn },
    Resource: { Table: { CatalogId: catalogId, DatabaseName: dbName, TableWildcard: {} } },
    Permissions: permissions,
  }));
}

export const handler = async (event) => {
  if (event.RequestType === 'Delete') {
    // Federation/grants intentionally retained on stack deletion. Logged
    // for operator awareness; manual cleanup if truly desired.
    console.log('Delete: leaving Glue federation and LF grants in place');
    return { PhysicalResourceId: event.PhysicalResourceId ?? 'noop' };
  }
  const p = event.ResourceProperties;
  if (p.Action === 'GrantPermissions') {
    await grantLf(p.CatalogId, p.DatabaseName, p.PrincipalArn, p.Permissions);
    return { PhysicalResourceId: `grant-${p.PrincipalArn}-${p.DatabaseName}` };
  }
  await ensureGlueFederation(p.ParentCatalogName, p.ChildCatalogName, p.TableBucketArn);
  return { PhysicalResourceId: `fed-${p.ChildCatalogName}` };
};
