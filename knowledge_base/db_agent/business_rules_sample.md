# Business Rules Sample

This document seeds the DB AGENT knowledge base. It demonstrates how rules can be expressed when updating the
vector store via the UpdateBusinessRules action group. The knowledge base only contains business rules—user
profiles and interaction history stay in DynamoDB and are queried through tools.

- Orders must be acknowledged within 5 minutes of receipt.
- Pricing adjustments require manager approval.
- UserData queries should prioritise verified customer records.
- Interaction-history entries should maintain chronological ordering for each session.
