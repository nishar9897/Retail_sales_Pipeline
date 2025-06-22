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
