# Challenges Faced During Data Loading - Retail Sales Data Pipeline Project

This document outlines the key challenges encountered while loading cleaned sales data into Cloud SQL using Apache Beam in the Retail Sales Data Pipeline project.

## Challenges Encountered

1. **Connection Timeout to Cloud SQL**
   - Issue: Faced `OperationalError` due to inability to connect to the Cloud SQL instance.
   - Root Cause: The server was not reachable from the runner environment, likely due to IP not being authorized or instance not running.

2. **No Iterator Returned Warning**
   - Issue: Beam warning: `No iterator is returned by the process method in InsertIntoSales`.
   - Root Cause: The `process` method in the DoFn class didn't return anything due to connection error in `setup`, causing the rest of the method to not execute.

3. **Null Product Entries in Sales Data**
   - Issue: Some sales records referred to products not present in the cleaned `products` table.
   - Resolution: A join was used during loading to filter out any mismatched product entries to avoid referential inconsistencies.

4. **Hardcoding in Scripts**
   - Issue: Early versions of the script had hardcoded project IDs and table names.
   - Resolution: Made the script dynamic by reading configuration from environment variables or constants.

5. **Cloud SQL Public Access Configuration**
   - Issue: Difficulty in configuring public access via authorized networks.
   - Resolution: Temporarily used `0.0.0.0/0` with strong passwords, but noted it should be replaced with specific IP ranges or Cloud SQL Proxy in production.

6. **Unclear Load Failures**
   - Issue: Failures occurred without clear error output due to pipeline silent failure downstream.
   - Resolution: Added structured error logging and fallback yields to aid in debugging.

7. **Missing Required Fields in Sales JSON**
   - Issue: Some sales entries were missing required fields like `CustomerID`, `ProductID`, etc.
   - Resolution: Applied checks in `CleanSales` transform to validate and skip malformed records.

---

Each challenge contributed to improving the robustness of the ETL process and better understanding of integrating Beam with Cloud SQL.


---

## Findings to build secure pipeline
## 1. Reading Password from GCP Secret Manager (Project: `de_practices`)

To securely manage sensitive information like database passwords, we used **Google Cloud Secret Manager**.

### Implementation Steps:
- Created a new secret (`db-password-prod`) in the `de_practices` project using GCP Secret Manager.
- Added the actual database password as a secret version.
- Granted `Secret Accessor` IAM permissions to the Dataflow worker service account.
- Accessed the password securely at runtime during pipeline execution via Secret Manager API.
- Ensured no credentials are hardcoded; secrets are centrally managed, auditable, and rotatable.

---

##  2. Dynamically Setting Input and Temp Location (for Cloud Composer / Cloud Function)

To enable dynamic and environment-specific execution, we made the input and temporary GCS paths configurable at runtime.

### Implementation Steps:
- Defined pipeline parameters (`--input`, `--temp_location`) for dynamic configuration.
- Designed the pipeline to accept and use these values at execution time.
- Passed parameters dynamically when triggering the job via **Cloud Composer** or **Cloud Functions**.
- Enabled reusability of the pipeline across dev/test/prod environments without modifying the code.

---

## 3. Connecting to On-Premise Network from GCP Dataflow

To fetch data from a non-GCP (on-premise) source system, we implemented a secure hybrid network setup.

### Implementation Steps:
- Established a **Cloud VPN** tunnel to securely connect GCP with the on-premise network.
- Created a **VPC Access Connector** to enable Dataflow workers to route traffic through the VPN.
- Configured **firewall rules** and **custom routes** to permit outbound access to the on-prem database IP and port.
- Used **private IP subnetworks** in Dataflow job configurations to ensure workers operate inside the connected VPC.
- Handled authentication using passwords stored in **Secret Manager**.
- Validated the setup with test jobs before moving to production-level automated runs.

---

