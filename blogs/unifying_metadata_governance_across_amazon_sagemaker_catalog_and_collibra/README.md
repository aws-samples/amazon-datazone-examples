# Unifying Metadata Governance Across Amazon SageMaker Catalog and Collibra

This repository contains the instructions and Python scripts to unify metadata governance across **Amazon SageMaker
Catalog** and **Collibra**.

> 📖 **Related blog post:** [Unifying Metadata Governance Across Amazon SageMaker Catalog and Collibra](#)

---

## 📦 Package Overview

This package includes:

1. `lambda/` – Contains Python scripts modeled as AWS Lambda function handlers that facilitate the integration between
   Amazon SageMaker Catalog and Collibra.
2. `template.yaml` – An AWS CloudFormation template that provisions the required infrastructure to deploy and automate
   AWS Lambda functions.
3. `build_lambda.sh` – A shell script that packages AWS Lambda source code into a deployable .zip archive.
4. `workflows/` - Contains workflows that need to be imported into Collibra. These workflows automate the tasks of synchronizing metadata and subscribing to assets.

---

## ⚙️ Configuration Details

### ✅ Prerequisites

Ensure the following prerequisites are met:

1. **Collibra Account** – Configured for ingesting metadata from AWS Glue and Amazon Redshift, and with permissions to
   import workflows.
2. **AWS Account** – With **SageMaker Data Catalog** set up for AWS Glue and Amazon Redshift metadata ingestion.

---

### 🚀 Deployment Steps

Follow these steps to set up metadata synchronization between Amazon SageMaker Catalog and Collibra. For a detailed
explanation, refer to the [blog post](#).

1. **Create a secret in AWS Secrets Manager**

   Use [this tutorial](https://docs.aws.amazon.com/secretsmanager/latest/userguide/create_secret.html) to create an **"
   Other type of secret"**, and store the Collibra credentials in the following format:

   ```json
   {
     "url": "account_name.collibra.com",
     "username": "collibra_user",
     "password": "collibra_user_password"
   }
   ```

2. **Build the AWS Lambda package**

   Run the following command to generate a deployable zip file:

   ```bash
   ./build_lambda.sh
   ```
3. **Upload to Amazon S3**

   Create an Amazon S3 bucket (if one doesn't exist), and upload the zip file generated in Step 2.

4. **Deploy the AWS CloudFormation Stack**

   Use the provided `template.yaml` file to create a AWS CloudFormation stack. This will provision the required roles,
   permissions, and AWS Lambda functions.

5. **Configure Amazon SageMaker Catalog access**

   After deployment, add the `SMUSCollibraIntegrationAdminRole` (created by the AWS CloudFormation stack) to the Amazon
   SageMaker Catalog Domain as:
    - A Domain user
    - A Producer project member
    - A Consumer project member

### 🚀 Workflow deployment in Collibra

Follow these steps to import and configure the workflows in Collibra:

1. **Import the workflow files in Collibra instance**

   Use [this tutorial](https://productresources.collibra.com/docs/collibra/latest/Content/Workflows/ManageWorkflows/ta_deploy-wf.htm)
   to upload and deploy the workflows in Collibra.


2. **Configure the parameters of the workflow**

   The workflows, especially the Subscription workflow, contains parameters related to the new asset types, attribute
   types, relation types and domain ids created for this solution. Because each Collibra instance has different UUIDs
   for these new types, we need to supply them in this list of parameters.
   Use [this tutorial](https://productresources.collibra.com/docs/collibra/latest/Content/Workflows/ManageWorkflows/co_configuration-variables.htm)
   to change the parameters.

3. **Define the rules for the workflow Subscription**

   The Subscription workflow needs to be applied to Assets only, and not Globally, and the asset to be applied is Table.
   Create a rule to apply Table as the asset where this workflow will deploy.
   Use [this tutorial](https://productresources.collibra.com/docs/collibra/latest/Content/Workflows/ManageWorkflows/co_general-wf-settings.htm)
   to add rule to the workflow. 
   
