# Recreate the Interaction-history table

Use `scripts/recreate_interaction_history_table.py` to drop and recreate the
`Interaction-history` table with the canonical Bedrock response attribute
(`system_response`). The seed file stores the full Bedrock response JSON so new
tables are created with example items that match the latest data model
(business `to_number` + local-time ISO timestamp, plus a `GSI_To_From` index on
`to_number`/`from_number`).

## Commands

```bash
# Set AWS credentials/region via environment variables or profiles first
python scripts/recreate_interaction_history_table.py \
  --region us-east-1 \
  --table-name Interaction-history \
  --seed-file assets/dynamodb/interaction_history_seed.json
```

The script will:

1. Delete the existing `Interaction-history` table if it is present.
2. Create the table with the PK/SK schema, on-demand billing, and NEW_IMAGE
   streams (as required by the state machine processors).
3. Seed the table with the sample item that already includes the Bedrock
   response payload under the canonical `system_response` attribute.

If you want to create the table without seed data, omit the `--seed-file`
argument.
