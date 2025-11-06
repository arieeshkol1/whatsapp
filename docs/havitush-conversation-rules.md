# Havitush Conversation Flow

This document summarises how the WhatsApp workflow enforces the Havitush booking policy.

1. Rules are stored in DynamoDB under the `RULESET#HAVITUSH` partition key.
2. The runtime seeds a default ruleset when the table is empty so a fresh environment
   immediately follows the strict onboarding script.
3. Every response includes customer details first, followed by the current order
   progress so the guest can review what has already been captured.
4. Entering the supervisor code `חביתוש123` switches the conversation into the
   management menu described by the rules document.

See `backend/common/rules_loader.py` for the loader and `backend/state_machine/processing/customer_flow.py`
for the state machine that persists and applies the rules.
