# F-5 - JournalId impersonation: a sibling environment adopts another environment's transaction (silent on encrypted DBs)

**Severity:** serious. On an **encrypted** database this is silent cross-environment data corruption. On an unencrypted one it is caught, but only after the victim's data file has already been written to, and the error blames the wrong file.
**Status:** open - not filed yet. Found 2026-08-07 on the rebased `RavenDB-24520` branch (all of 27156 / 27166 / 27168 / 27220 / 27278 in place).
**Evidence:** observed (deterministic test, both variants reproduce) + derived (code path).
**Shared context:** see [README.md](README.md). Generalizes [F-2](F-2-silent-journalid-loss.md) - same unprotected field, worse outcome.

## Claim

`JournalId` (offset 136 of `TransactionHeader`) decides which environment replays a transaction from a shared journal, but it is covered by **no integrity mechanism at all**. Rewriting those 16 bytes to a *sibling branch's* id produces a transaction that passes every validation and is then replayed by the wrong environment.

## Mechanism (code-confirmed)

Three independent gaps line up:

1. **The hash does not cover it.** `JournalReader.ValidatePagesHash` ([JournalReader.cs:878](../../../../src/Voron/Impl/Journal/JournalReader.cs)) hashes the payload only, seeded with `TransactionId`. Every other header field is outside the hash.

2. **The AEAD does not cover it either.** In `DecryptTransaction` ([JournalReader.cs:475-485](../../../../src/Voron/Impl/Journal/JournalReader.cs)) the additional authenticated data is `page` with length `TransactionHeader.SizeOf - TransactionHeader.NonceOffset` = 192 - 152 = **40 bytes**, i.e. header range `[0, 40)`. `JournalId` at 136 is outside it. Worse, the subkey is derived from `TransactionId` alone (`crypto_kdf_derive_from_key(subKey, subKeyLen, (ulong)num, ctx, mk)` where `num = TransactionId`) and **not** from the environment identity - so two different environments that reach the same transaction id derive the *same* subkey. That is precisely why a sibling can successfully decrypt another environment's transaction.

3. **The sequence check can be satisfied by accident.** Per-environment transaction id counters run in parallel and independently, so branch A's transaction id N is frequently exactly what branch B expects next. `VerifyTransactionSequence` then sees a contiguous chain (`txIdDiff == 1`) and accepts the transaction as B's own.

Root cause in one line: **v8 introduced `JournalId` as a routing-critical header field and extended neither the transaction hash nor the AEAD authenticated range to cover it.**

## Observed behaviour

Test: `FastTests.Voron.SharedJournal.RavenDB_24520.ImpersonatedJournalIdMustNotMakeSiblingBranchAdoptForeignTransaction`. Layout is `root tx, A boot, link record, B boot, a1(A), b1(B), victim(A, last tx)`, with `a1` putting A's counter one ahead so A's trailing transaction id is exactly what B expects. The corruption is a single 16-byte write of B's Guid over the victim's `JournalId` - payload, hash and MAC untouched.

| Variant | Outcome |
|---|---|
| **Encrypted** | **Branch B opens successfully and exposes branch A's tree `treeA`.** No exception, no error, nothing logged. Recovery takes the root tree from the last applied transaction header, so B is now serving A's data. |
| Unencrypted | B's data file **is written to** with A's pages, then recovery aborts with `InvalidDataException: Invalid checksum for page 0, data file ...\Raven.voron might be corrupted`. B never opens. |

Both variants fail the test; the assertion that survives in both is that B's data file was physically mutated before anything objected.

The unencrypted backstop is the post-recovery page-checksum sweep in `WriteAheadJournal.RecoverDatabase` ([WriteAheadJournal.cs:476](../../../../src/Voron/Impl/Journal/WriteAheadJournal.cs)), and it is guarded by:

```csharp
if (_env.Options.Encryption.IsEnabled == false) // for encryption, we already use AEAD, so no need
```

That justification is unsound here: the AEAD does not authenticate the field that decides ownership, so on encrypted databases the only check that would have caught this is deliberately skipped. `SkipChecksumValidationOnDatabaseLoading` removes the backstop on unencrypted databases too.

Two secondary problems, both independent of encryption:

- **The data file is mutated before rejection.** `ReadOneTransactionToDataFile` writes pages straight to the data file during replay (`WritePageToFile`), so the foreign pages land before the sweep runs. A failed open is not a no-op.
- **The error names the wrong file.** It reports the *data file* as possibly corrupt when the journal is what was damaged, which sends an operator at the wrong artifact.

## Fault model (be honest about this)

A random single-bit flip in `JournalId` produces a Guid that belongs to nobody - that is [F-2](F-2-silent-journalid-loss.md) (silent drop), not this. Landing on a *sibling's* Guid by chance is not a realistic bit-rot outcome. The realistic triggers are:

- **misdirected / stale-block writes** - a real storage fault class where a block is written to the wrong LBA or a stale block is returned; at 4KB granularity this moves whole headers between positions, which is this bug's shape;
- **deliberate tampering** - the shared journal is a plain file, and 16 bytes redirect a committed transaction into a different index's storage;
- and any future product bug that computes `JournalId` incorrectly, since nothing downstream would detect it.

The demonstrated defect is the general one: **ownership is trusted without integrity.** The 16-byte edit is the proof vehicle.

## Blast radius

Shared journals are index-only, so documents are never at risk and the damaged index is rebuildable. On an encrypted database the practical impact is an index silently serving another index's data until someone notices - queries return wrong results rather than errors. That is worse than a faulty index, because nothing signals it.

## Repro

```bash
dotnet test test/FastTests -c Release --filter "FullyQualifiedName~RavenDB_24520.ImpersonatedJournalId"
```
Expect both variants to fail on the current build. The encrypted variant is the one that demonstrates silent corruption; read the test output lines (`branch B data file mutated ...`) to see which stage caught it.

## Fix direction (not implemented - needs a decision)

1. **Cover `JournalId` with the transaction hash** - cheapest and closes the unencrypted case. Changes the on-disk hash contract, so it needs a compatibility story for journals written by earlier v8 builds (legacy transactions already carry `Guid.Empty` and are special-cased).
2. **Extend the AEAD authenticated data to the whole header, or derive the subkey from `JournalId` as well as `TransactionId`.** Deriving from both is attractive: it makes cross-environment decryption impossible by construction rather than by check, and it removes the same-subkey-for-same-txid property.
3. Independently: stop claiming AEAD covers the ownership field - either re-enable the checksum sweep for encrypted databases or narrow that comment to what is actually authenticated.
4. Independently: make the recovery error name the journal and position, not the data file.

Item 2 is the one that fixes the encrypted case properly. Items 3 and 4 are cheap and worth doing regardless.
