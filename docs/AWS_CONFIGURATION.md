# AWS CONFIGURATION

In order to correctly configure the AWS account and manual steps (only once), follow these steps.

## Configure Secrets Manager Secret

Create an AWS Secret that will contain the required tokens/credentials for connecting AWS and Meta APIs.

This can be done with the following AWS CLI command:

TODO: Add AWS CLI command with the example secret creation (with necessary keys/values template)

## Create the deployment role

Use a dedicated IAM role when deploying the CDK stacks so you can separate the WhatsApp bot from other workloads. The repository provides JSON templates under `docs/iam/` for both the deployment role and the GitHub Actions user:

* `whatsapp_deployment_role_policy.json` – attach this as an **inline permissions policy** (or turn it into a customer-managed policy such as `whatsapp_deployment_policy`) on the deployment role so CDK can create/update the resources the project needs.
* `whatsapp_deployment_role_trust_policy.json` – use this as the **trust policy** for the role. The template is pre-populated so only the IAM user `github-cicd` in account `960915223703` can assume the role and it already allows both `sts:AssumeRole` and `sts:TagSession` so GitHub Actions can attach the session tags it sends automatically. If your deployment identity is different, change the `Principal.AWS` value to the ARN of the user or role that should be able to assume the deployment role. When you prefer to authorize every principal in the account, replace the value with `arn:aws:iam::<YOUR_ACCOUNT_ID>:root` and optionally add a `Condition` block (for example, to require MFA or restrict to specific users). Reapply the template with `aws iam update-assume-role-policy` if the role already exists so the `sts:TagSession` action is present.
* `github_cicd_user_policy.json` – attach this as an **inline policy on the `github-cicd` IAM user** (or use it as a customer-managed policy) so the user can assume the deployment role. The policy includes the `sts:TagSession` permission required by the GitHub Actions runner, which automatically adds session tags when it calls `sts:AssumeRole`.

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
      },
      "Action": [
        "sts:AssumeRole",
        "sts:TagSession"
      ]
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

# If the role already exists, refresh the trust policy so it includes sts:TagSession
aws iam update-assume-role-policy \
  --role-name WhatsAppCdkDeploy \
  --policy-document file://docs/iam/whatsapp_deployment_role_trust_policy.json

# Attach the managed policy to the deployment role
aws iam attach-role-policy \
  --role-name WhatsAppCdkDeploy \
  --policy-arn arn:aws:iam::960915223703:policy/whatsapp_deployment_policy

# Grant the GitHub Actions user permission to assume the role (inline policy example)
aws iam put-user-policy \
  --user-name github-cicd \
  --policy-name github_cicd_assume_whatsapp_role \
  --policy-document file://docs/iam/github_cicd_user_policy.json
```

Update the example account ID (`960915223703`) if you store the policy in a different account. Re-run `create-policy` only when you intentionally update the permissions JSON; otherwise you can keep attaching the existing `whatsapp_deployment_policy` to new roles.

## Connect the GitHub Actions pipeline

Once the deployment role exists, wire the GitHub Actions workflow (`.github/workflows/deploy.yml`) so it can assume the role during the `cdk` steps.

1. **Grant the `github-cicd` IAM user permission to assume the deployment role**
   * Attach the policy in `docs/iam/github_cicd_user_policy.json` to the user. Update the `Resource` ARN in that template if your role name or account ID differs. Without the `sts:TagSession` permission included in the template, the GitHub Actions workflow fails with `User ... is not authorized to perform: sts:TagSession` when it assumes the role.
   * **Console refresh:** after pasting the JSON into a new policy, confirm the policy summary lists both `sts:AssumeRole` and `sts:TagSession`, attach it to `github-cicd`, and remove any older inline/managed policies that might still omit `sts:TagSession`.
   * **CLI inline policy update:**
     ```bash
     aws iam put-user-policy \
       --user-name github-cicd \
       --policy-name github_cicd_assume_whatsapp_role \
       --policy-document file://docs/iam/github_cicd_user_policy.json
     ```
     This overwrites the inline policy on the user with the correct permissions.
   * **CLI managed policy update:**
     ```bash
     aws iam create-policy \
       --policy-name github-cicd-whatsapp-assume-role \
       --policy-document file://docs/iam/github_cicd_user_policy.json || true

     aws iam attach-user-policy \
       --user-name github-cicd \
       --policy-arn arn:aws:iam::960915223703:policy/github-cicd-whatsapp-assume-role

     aws iam create-policy-version \
       --policy-arn arn:aws:iam::960915223703:policy/github-cicd-whatsapp-assume-role \
       --policy-document file://docs/iam/github_cicd_user_policy.json \
       --set-as-default
     ```
     The `|| true` keeps the script moving if the policy already exists; the subsequent `create-policy-version` call replaces the document so the user receives the updated permissions immediately.
     If the workflow still reports `sts:TagSession` authorization errors, run `aws iam update-assume-role-policy --role-name whatsapp_deployment_role --policy-document file://docs/iam/whatsapp_deployment_role_trust_policy.json` to make sure the trust policy allows tagging the session.
2. **Create access keys for the `github-cicd` IAM user**
   * In the AWS console open **IAM → Users → github-cicd → Security credentials** and create an access key. Download the CSV because you cannot view the secret again after you leave the page. Rotate the key periodically.
3. **Add the keys as GitHub secrets**
   * In your GitHub repository go to **Settings → Secrets and variables → Actions** and add the following **Repository secrets**:
     * `AWS_ACCESS_KEY_ID` – the access key ID you just generated for `github-cicd`.
     * `AWS_SECRET_ACCESS_KEY` – the corresponding secret access key.
     * `DEV_AWS_ACCOUNT_ID` – set to `960915223703` (or the account ID that owns the deployment role).
     * `DEV_AWS_DEPLOY_ROLE` – set to the name of the role you created (for example `whatsapp_deployment_role`).
     * If you plan to trigger the workflow with the `prod` environment input, also add `PROD_AWS_ACCOUNT_ID` and `PROD_AWS_DEPLOY_ROLE` with the appropriate values.
4. **Trigger the deployment workflow**
   * Open the **Actions** tab in GitHub, select **Deploy**, and click **Run workflow**. Pick the target environment (`dev` or `prod`) and start the run. The `aws-actions/configure-aws-credentials` step uses the stored access keys to assume `arn:aws:iam::<ACCOUNT_ID>:role/<ROLE_NAME>` and deploy the CDK stacks.

If the workflow fails with an authorization error, verify that the `github-cicd` user has permission to call `sts:AssumeRole` on `whatsapp_deployment_role`, the secrets contain the correct values, and the role’s trust policy still matches the user ARN shown above.

### Verify the GitHub CI permissions from the CLI

After attaching the policies, you can double-check that the `github-cicd` user is authorized to assume the deployment role before triggering a workflow. The following commands use the AWS CLI to validate both the permissions and the trust relationship:

1. **Simulate the IAM permissions**
   * Run the policy simulator to confirm `sts:AssumeRole` and `sts:TagSession` are granted for the target role:
     ```bash
     aws iam simulate-principal-policy \
       --policy-source-arn arn:aws:iam::960915223703:user/github-cicd \
       --action-names sts:AssumeRole sts:TagSession \
       --resource-arns arn:aws:iam::960915223703:role/whatsapp_deployment_role
     ```
     The `EvaluationResults` array should report `Allowed` for both actions. If either action is `ImplicitDeny`, re-check the user policy.
2. **Confirm the trust policy references the user**
   * Fetch the role and inspect the `AssumeRolePolicyDocument`:
     ```bash
     aws iam get-role \
       --role-name whatsapp_deployment_role \
       --query 'Role.AssumeRolePolicyDocument.Statement'
     ```
     Ensure the `Principal.AWS` field includes `arn:aws:iam::960915223703:user/github-cicd` (or the identity you expect to allow) and that the `Action` list contains both `sts:AssumeRole` and `sts:TagSession`.
3. **Attempt an STS assume-role call**
   * Using the profile that holds the `github-cicd` access key (or by supplying `--access-key-id/--secret-access-key`), call STS:
     ```bash
    aws sts assume-role \
      --role-arn arn:aws:iam::960915223703:role/whatsapp_deployment_role \
      --role-session-name github-cicd-test \
      --profile github-cicd
    ```
    A successful response returns temporary credentials. If the command fails with `AccessDenied` or `Not authorized to perform: sts:TagSession`, update the user policy or trust relationship accordingly.

### Why the CDK only targets one environment per run

The repository ships a single CDK app that chooses between the `dev` and `prod` configurations at runtime based on the `DEPLOYMENT_ENVIRONMENT` environment variable. When the app starts it reads the context block from `cdk.json`, looks up the configuration object that matches the current environment, and only instantiates one stack whose name ends with that environment suffix (for example, `aws-wpp-chatbot-api-dev`).

The GitHub Actions workflow sets `DEPLOYMENT_ENVIRONMENT` from the workflow-dispatch input, so a manual run with `dev` produces only the `dev` stack and a separate run with `prod` produces only the `prod` stack. The workflow has mutually exclusive credential steps guarded by `if: github.event.inputs.environment == 'dev'`/`'prod'`, which ensures a single run never assumes both roles or synthesizes both environments.

If you see templates for both environments in your history it is typically because you triggered two separate workflow runs (one for each environment) or executed `cdk synth` twice locally after changing `DEPLOYMENT_ENVIRONMENT`. Running `cdk synth` with a single environment variable value will only build the stack for that environment, so there is no risk of the pipeline modifying both environments in a single run.

## Connect from your workstation to the management account

If you prefer to deploy from your laptop (or another CI system) instead of GitHub Actions, create an AWS CLI profile that assumes the `whatsapp_deployment_role` in account `960915223703` and use it for all CDK commands.

1. **Install/upgrade the AWS CLI**
   * Follow the [AWS CLI installation guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) so the `aws` command is available locally.
2. **Configure a base profile with the `github-cicd` credentials**
   * Run `aws configure --profile github-cicd` and supply the access key ID and secret access key for the IAM user. Leave the region/output prompts blank if you prefer to set them elsewhere.
3. **Add an assume-role profile for deployments**
   * Edit (or create) `~/.aws/config` and add:
     ```ini
     [profile whatsapp-bot]
     role_arn = arn:aws:iam::960915223703:role/whatsapp_deployment_role
     source_profile = github-cicd
     region = us-east-1
     ```
     Adjust the region if you deploy to another AWS Region.
4. **Verify access to the management account**
   * Run `aws sts get-caller-identity --profile whatsapp-bot`. The returned `Account` field should be `960915223703` and the `Arn` should end with `role/whatsapp_deployment_role`.
5. **Bootstrap and deploy with the role**
   * Execute `cdk bootstrap --profile whatsapp-bot` once so CDK creates its deployment resources in the management account.
   * Run `cdk synth --profile whatsapp-bot` to confirm the stacks synthesize.
   * Deploy with `cdk deploy --profile whatsapp-bot` whenever you are ready.

If you use environment variables instead of named profiles, set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` by calling `aws sts assume-role --role-arn arn:aws:iam::960915223703:role/whatsapp_deployment_role --role-session-name whatsapp-cli` before running CDK commands. Export the returned credentials and repeat the bootstrap/deploy steps above.

## Pull the latest repository changes before deploying

Before triggering the deployment workflow for the first time (or any time afterward), make sure your local checkout contains the latest commits from the default branch so you deploy the exact code that lives in GitHub.

1. **Fetch the newest commits**
   * From your repository directory run `git fetch origin` to download changes without modifying your working tree.
2. **Switch to the branch you plan to deploy**
   * If you deploy from `main`, run `git checkout main`.
3. **Fast-forward your branch to the remote state**
   * Run `git pull --ff-only origin main` to update your branch without creating merge commits. Replace `main` with the branch name you want to deploy when testing feature branches.
4. **Verify you are up to date**
   * Execute `git status` to ensure the branch shows “up to date with 'origin/<branch>'” and there are no unintended local changes. Resolve outstanding changes (commit, stash, or discard) before continuing.
5. **Push any local commits you want deployed**
   * Use `git push origin <branch>` so the GitHub Actions deployment workflow runs against the newest code on GitHub. The workflow always pulls directly from the repository, so pushing your changes is required for them to be included.

After your local checkout matches the remote branch, you can run the GitHub Actions **Deploy** workflow or execute the `cdk` commands from your workstation profile outlined above.
