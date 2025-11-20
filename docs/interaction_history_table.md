# Recreate the Interaction-history table

Use `scripts/recreate_interaction_history_table.py` to drop and recreate the
`Interaction-history` table with the Bedrock response attributes expected by the
processing pipeline (`Response`/`System_Response`). The seed file stores the
full Bedrock response JSON so new tables are created with example items that
match the latest data model.

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
3. Seed the table with the sample item that already includes Bedrock response
   payloads under both `Response` and `System_Response`.

If you want to create the table without seed data, omit the `--seed-file`
argument.
