<!-- Suggested GitHub issue title: replay: Enforce priority ordering within each EntryBatch -->

# Enforce priority ordering within each EntryBatch

## Summary

The original vision for Solana was building Decentralized NASDAQ. That remains our goal. An exchange must of course be fast and cheap; increasing bandwidth and reducing latency (IBRL) is essential. But those are table stakes at this point. Performance alone is necessary but not sufficient to build a great exchange. An exchange must also provide fairness, determinism, predictability, and a favorable market structure that participants can inspect and easily reason about. Without these properties, onboarding new traders and obtaining favorable regulatory outcomes becomes much harder.

By my count, at least 13 distinct scheduler implementations are active on Solana mainnet today, and I am aware of several more that will be coming online soon. Scheduling was hard enough to understand when leader software was far more homogeneous and almost every leader ran Jito-Agave. With this much diversity, it is increasingly difficult to know how any particular leader will prioritize transactions, resolve conflicts, form batches, and execute them.

I maintain certain tools to help searchers understand this behavior. Over the last seven months, I have found it increasingly difficult to keep these tools current with the scheduling nuances that continue to appear month after month. Even extremely sophisticated on-chain traders, who have a much more tangible incentive than I do to stay current, tell me that they are having the same problem.

Scheduler experimentation has produced valuable performance work and exposed many transaction-pipeline bugs. But performance does not require leaving transaction order within each `EntryBatch` entirely to scheduler discretion.

This proposal makes priority ordering within a completed `EntryBatch` a block validity rule. Producers still control inclusion and `EntryBatch` boundaries, so this does not guarantee fair ordering. The leader must record transactions in priority order. Replay checks that order and marks the block dead if it is violated.

## Background

When a validator receives a block, it reconstructs the resulting state by replaying the transactions in that block. Transactions arrive at replay in groups called `EntryBatch`.

Today, replay executes each `EntryBatch` in the order recorded by the leader.

## Design

Here is the entire change:

```text
 1  today, upon receiving a complete EntryBatch:
 2      replay(entry_batch.transactions)                                      // order recorded by the leader

 3  with this proposal, upon receiving a complete EntryBatch:
 4      if not is_sorted(entry_batch.transactions, by priority descending):
 5          return ProtocolViolation                                          // mark the block dead
 6      replay(entry_batch.transactions)                                      // unchanged: order recorded by the leader
```

`priority` is the same score Agave already uses to rank transactions (`calculate_priority_and_cost_v1`). Equal-priority transactions are ordered by signature ascending.

The leader's scheduler is expected to construct each `EntryBatch` in this order. Replay only enforces the rule; it does not repair invalid leader output.

That is the whole consensus change: an out-of-order `EntryBatch`, which replay accepts today, becomes a protocol violation. Replay does not reorder transactions. It marks the block dead if the leader did not record an `EntryBatch` in canonical order; otherwise, it replays the transactions exactly as it does today.

The order recorded in every valid `EntryBatch` is therefore the canonical order for the ledger, replay, and RPC. Validators remain free to use different hardware or internal scheduling strategies. A leader may execute transactions locally or leave all execution to replay, but the block it produces must be valid in canonical order.

The ordering check occurs after the complete `EntryBatch` is available and before any transaction in it is executed.

## Non-goals and limitations

- This does not constrain inclusion or provide slot-global ordering. Producers still choose which transactions enter each `EntryBatch`.
- This does not prescribe leader execution, replay parallelism, hardware configuration, or new block limits.
- This does not prevent a leader from prioritizing its own transactions by paying fees to itself. Fee burn makes this nonzero cost, but the collector recovers its own fee deposit; further changes are required to address that behavior.



## Impact

**Bundles:** It is still possible to produce bundle like behavior after this change but it may require some changes. For example fees may have to be charged in priority fees and distributed across the entire bundle. Revert protection is still possible for leaders who execute transactions.

**Long term plan for bundles**: The plan for bundles is eventually to remove them and replace them with other things. Bundles provide two features.

1. All or nothing

2. revert protection.

   Transaction size is increasing (even past 4k) so that many of the benign use cases of all or nothing bundles will be possible within in a single transaction. As far as revert protection goes. The revenue equivalence theorem tells us that revenue from all pay (ordinary PGA with no revert protection) should be similar to that of a first price (rever protected) auction. The possible exception to this would be for back runs. I think it may make sense to put some kind of success/failure fee in the protocol eventually but it significantly complicates the journey to in protocol ordering. I think we should take a look at adding it after we have fully achieved in protocol ordering.

## References

- [Bankless leaders roadmap](https://github.com/solana-foundation/solana-improvement-documents/issues/324)
