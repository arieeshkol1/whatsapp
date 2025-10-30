# AWS CONFIGURATION

In order to correctly configure the AWS account and manual steps (only once), follow these steps.

## Configure Secrets Manager Secret

Create an AWS Secrets Manager entry (for example `/dev/aws-whatsapp-chatbot`) that will contain the
required tokens/credentials for connecting AWS and Meta APIs. The deployed Lambdas reference the
secret through the `SECRET_NAME` environment variable, so make sure to store the values in Secrets
Manager rather than committing them to the repository or setting them directly on the functions.

The helper shipped in this repository creates or updates the JSON document for you:

```bash
python backend/create_secret_cli.py \
  --secret-name /dev/aws-whatsapp-chatbot \
  --verify-token <verify-token-shared-with-meta> \
  --meta-token <long-lived-meta-token> \
  --phone-number-id <meta-phone-number-id>
```

By default the script writes the following keys into the secret:

```json
{
  "AWS_API_KEY_TOKEN": "...",
  "META_TOKEN": "...",
  "META_FROM_PHONE_NUMBER_ID": "..."
}
```

If you need to store additional values (for example, `META_BASE_URL`), pass
`--extra META_BASE_URL=https://graph.facebook.com/v20.0` and repeat `--extra` for each additional
key/value pair. The CLI prints only the key names after it runs so your sensitive values stay hidden
from logs. You can also create the secret manually from the AWS console as long as you keep the same
JSON structure.

## Create the deployment role

Use a dedicated IAM role when deploying the CDK stacks so you can separate the WhatsApp bot from other workloads. The repository provides two JSON templates under `docs/iam/`:

* `whatsapp_deployment_role_policy.json` – attach this as an **inline permissions policy** (or turn it into a customer-managed policy such as `whatsapp_deployment_policy`) on the deployment role so CDK can create/update the resources the project needs.
* `whatsapp_deployment_role_trust_policy.json` – use this as the **trust policy** for the role. The template is pre-populated so only the IAM user `github-cicd` in account `960915223703` can assume the role. If your deployment identity is different, change the `Principal.AWS` value to the ARN of the user or role that should be able to assume the deployment role. When you prefer to authorize every principal in the account, replace the value with `arn:aws:iam::<YOUR_ACCOUNT_ID>:root` and optionally add a `Condition` block (for example, to require MFA or restrict to specific users).

### Console walkthrough

1. Go to **IAM → Roles → Create role** and choose **Custom trust policy**.
2. Paste the trust policy template (with your account ID substituted) into the editor and continue.
3. On the permissions step either:
   * Choose **Add permissions → Attach policies** and select the managed policy you created earlier (for example `whatsapp_deployment_policy`).
   * Or choose **Add permissions → Create inline policy → JSON** and paste the permissions policy template if you prefer to keep it inline.
4. Review and create the role (for example, `WhatsAppCdkDeploy`).
5. Configure your AWS CLI profile to assume this role when running `cdk bootstrap`, `cdk synth`, and `cdk deploy`.

If you are creating the role through infrastructure-as-code (e.g., CloudFormation), provide the same documents via the role's `AssumeRolePolicyDocument` (trust policy) and `Policies` (permissions policy) sections.

> **Tip:** If the console reports `Has prohibited field Resource`, it means the permissions policy was pasted into the trust policy editor. Ensure the trust relationship uses `whatsapp_deployment_role_trust_policy.json` and the inline or managed permissions use `whatsapp_deployment_role_policy.json` as their source document.

#### Example: restrict the deployment role to the `github-cicd` IAM user

If your management account ID is `960915223703` and you want only the IAM user named `github-cicd` to assume the deployment role, you can use the provided trust policy as-is:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowGithubCicdUser",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::960915223703:user/github-cicd"
      }
      "Action": "sts:AssumeRole"
    }
  ]
}
```

To authorize additional principals, turn the `AWS` value into an array of ARNs (for example, `"AWS": ["arn:aws:iam::<ACCOUNT_ID>:user/github-cicd", "arn:aws:iam::<ACCOUNT_ID>:role/AnotherRole"]`) or switch back to the account root ARN and add `Condition` blocks that match the identities you want to allow.

### Optional AWS CLI workflow

If you would rather automate the setup, you can create the customer-managed policy and attach it to the role from the CLI using the templates in this repository:

```bash
# Create or update the permissions policy once
aws iam create-policy \
  --policy-name whatsapp_deployment_policy \
  --policy-document file://docs/iam/whatsapp_deployment_role_policy.json

# Create the role with the trust policy
aws iam create-role \
  --role-name WhatsAppCdkDeploy \
  --assume-role-policy-document file://docs/iam/whatsapp_deployment_role_trust_policy.json

# Attach the managed policy to the deployment role
aws iam attach-role-policy \
  --role-name WhatsAppCdkDeploy \
  --policy-arn arn:aws:iam::960915223703:policy/whatsapp_deployment_policy
```

Update the example account ID (`960915223703`) if you store the policy in a different account. Re-run `create-policy` only when you intentionally update the permissions JSON; otherwise you can keep attaching the existing `whatsapp_deployment_policy` to new roles.

## Connect the GitHub Actions pipeline

Once the deployment role exists, wire the GitHub Actions workflow (`.github/workflows/deploy.yml`) so it can assume the role during the `cdk` steps.

1. **Create access keys for the `github-cicd` IAM user**
   * In the AWS console open **IAM → Users → github-cicd → Security credentials** and create an access key. Download the CSV because you cannot view the secret again after you leave the page. Rotate the key periodically.
2. **Add the keys as GitHub secrets**
   * In your GitHub repository go to **Settings → Secrets and variables → Actions** and add the following **Repository secrets**:
     * `AWS_ACCESS_KEY_ID` – the access key ID you just generated for `github-cicd`.
     * `AWS_SECRET_ACCESS_KEY` – the corresponding secret access key.
     * `DEV_AWS_ACCOUNT_ID` – set to `960915223703` (or the account ID that owns the deployment role).
     * `DEV_AWS_DEPLOY_ROLE` – set to the name of the role you created (for example `whatsapp_deployment_role`).
     * If you plan to trigger the workflow with the `prod` environment input, also add `PROD_AWS_ACCOUNT_ID` and `PROD_AWS_DEPLOY_ROLE` with the appropriate values.
3. **Trigger the deployment workflow**
   * Open the **Actions** tab in GitHub, select **Deploy**, and click **Run workflow**. Pick the target environment (`dev` or `prod`) and start the run. The `aws-actions/configure-aws-credentials` step uses the stored access keys to assume `arn:aws:iam::<ACCOUNT_ID>:role/<ROLE_NAME>` and deploy the CDK stacks.

If the workflow fails with an authorization error, verify that the `github-cicd` user has permission to call `sts:AssumeRole` on `whatsapp_deployment_role`, the secrets contain the correct values, and the role’s trust policy still matches the user ARN shown above.
