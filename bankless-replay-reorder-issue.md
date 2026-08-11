<!-- Suggested GitHub issue title: replay: Decouple transaction execution order from ledger order -->

# Decouple transaction execution order from ledger order

## Summary

The original vision for Solana was building Decentralized NASDAQ. That remains our goal. An exchange must of course be fast and cheap; increasing bandwidth and reducing latency (IBRL) is essential. But those are table stakes at this point. Performance alone is necessary but not sufficient to build a great exchange. An exchange must also provide fairness, determinism, predictability, and a favorable market structure that participants can inspect and easily reason about. Without these properties, onboarding new traders and obtaining favorable regulatory outcomes becomes much harder.

By my count, at least 13 distinct scheduler implementations are active on Solana mainnet today, and I am aware of several more that will be coming online soon. Scheduling was hard enough to understand when leader software was far more homogeneous and almost every leader ran Jito-Agave. With this much diversity, it is increasingly difficult to know how any particular leader will prioritize transactions, resolve conflicts, form batches, and execute them.

I maintain certain tools to help searchers understand this behavior. Over the last seven months, I have found it increasingly difficult to keep these tools current with the scheduling nuances that continue to appear month after month. Even extremely sophisticated on-chain traders, who have a much more tangible incentive than I do to stay current, tell me that they are having the same problem.

Scheduler experimentation has produced valuable performance work and exposed many transaction-pipeline bugs. But performance does not require giving schedulers free rein to delay, reprioritize, or otherwise manipulate user transactions.

This proposal makes execution order within a completed `EntryBatch` a replay rule. Producers still control inclusion and `EntryBatch` boundaries, so this does not guarantee fair ordering. A producer should normally record transactions in priority order, but replay sorts them whether or not the producer does so and whether or not the leader executes locally.

## Background

When a validator receives a block, it reconstructs the resulting state by replaying the transactions in that block. Transactions arrive at replay in groups called `EntryBatch`.

Today, replay executes each `EntryBatch` in the order recorded by the leader.

## Design

Here is the entire change:

```text
 1  today, upon receiving a complete EntryBatch:
 2      replay(entry_batch.transactions)                                      // order recorded by the leader

 3  with this proposal, upon receiving a complete EntryBatch:
 4      transactions = sort(entry_batch.transactions, by priority descending) // ordered by priority fee per CU
 5      replay(transactions)
```

`priority` is the same score Agave already uses to rank transactions (`calculate_priority_and_cost_v1`). Equal-priority transactions have ties broken by comparing signatures.

That is the whole consensus change. Replay still works as it does today and validators remain free to use different hardware or internal scheduling strategies. A leader may execute transactions locally or leave all execution to replay.

The block itself is not reordered: block and RPC presentation remain in the order recorded by the leader. All transactions in the `EntryBatch` are verified before any is executed.

Reordering can change which transaction sees a depleted fee payer or an advanced durable nonce. SIMDs 0192, 0290, and 0297, or equivalent transaction-level failure semantics, must therefore activate first so these cases fail the transaction rather than the entire block.

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
- [SIMD-0192: Relax transaction loading constraints](https://github.com/solana-foundation/solana-improvement-documents/pull/192)
- [SIMD-0290: Relax fee-payer constraints](https://github.com/solana-foundation/solana-improvement-documents/pull/290)
- [SIMD-0297: Relax durable-nonce constraints](https://github.com/solana-foundation/solana-improvement-documents/pull/297)
