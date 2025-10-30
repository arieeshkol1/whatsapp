# CI/CD Automation Notes

The repository includes a dedicated GitHub Actions workflow (`.github/workflows/codex-auto-approve-deploy.yml`) that is triggered whenever Codex opens or updates a pull request. The workflow automatically leaves an approval review and deploys the pull request revision to the `dev` AWS environment.

## Why a pull request is still required

GitHub only allows workflows to run with elevated permissions (for example, the ability to assume the AWS deployment role) from pull request contexts. Direct pushes from untrusted identities are deliberately restricted so that a compromised token cannot ship code straight to the default branch. Because of that security model, the automation must run from a pull request event even if the change originated from Codex.

The workflow therefore performs the following steps:

1. Detect whether the head commit author email matches `codex@openai.com`.
2. Auto-approve the pull request if it was authored by Codex.
3. Deploy the pull request's HEAD commit to the `dev` environment using CDK.

This means no manual review is needed for Codex-authored pull requests, but the pull request object is still the vehicle that grants the workflow the permissions it needs. Once the workflow finishes, you can merge the pull request (or configure branch protection to auto-merge after approval) in the GitHub UI.

## Recommended workflow for Codex changes

1. Push the changes to a feature branch.
2. Open a pull request targeting `main`.
3. Wait for the **Auto approve and deploy Codex pull requests** workflow to finish. It will deploy the revision to `dev` automatically.
4. Merge the pull request (manually or through an auto-merge rule) when you are ready for `main` to advance.

If you prefer fully automated merging, enable GitHub's “Allow auto-merge” setting on the repository and select it on Codex pull requests. The existing workflow already supplies the approval required for auto-merge to proceed.
