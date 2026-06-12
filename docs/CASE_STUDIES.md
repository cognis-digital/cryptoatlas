# cryptoatlas — Case Studies: Cybersecurity Risk and Why Chains Drop

Ten public, well-documented incidents that show how **security failures translate into price collapse** — and why a security-risk discount sits on most chains and tokens. Each study links the event to the kind of public, entity-level data `cryptoatlas` indexes (sanctioned addresses, seizure wallets, bridge/exchange/contract labels) so you can investigate it yourself.

> All figures are from public reporting and on-chain records. Entity-level only; no private-individual data.

---

## 1. Mt. Gox (2014) — custody failure, ~850,000 BTC
The dominant BTC exchange lost ~850k BTC (~$450M then) to a slow, undetected drain plus operational insolvency. BTC fell roughly 50% over the following year and the overhang of recovered/“trustee” coins suppressed price for a decade.
**Security root cause:** centralized custody, no proof-of-reserves, undetected hot-wallet leakage.
**cryptoatlas angle:** exchange + estate/seizure wallet labels; track trustee distribution addresses.

## 2. The DAO (2016) — reentrancy exploit, ~3.6M ETH
A recursive-call (reentrancy) bug drained ~3.6M ETH (~$60M) from Ethereum’s flagship smart contract, forcing the contentious hard fork that split ETH and ETC.
**Security root cause:** unaudited contract logic; state updated after external call.
**cryptoatlas angle:** contract-label category; the exploit + fork are the archetype of smart-contract risk priced into every L1.

## 3. Coincheck (2018) — hot-wallet theft, $530M NEM
~$530M of NEM (XEM) stolen from an internet-connected hot wallet with no multisig. NEM dropped sharply; the hack marked the top of the 2018 cycle for many alts.
**Security root cause:** funds in a single hot wallet, no cold storage / multisig.
**cryptoatlas angle:** exchange wallet labels; the thief’s tagged addresses were tracked publicly.

## 4. Terra / LUNA (2022) — design failure, ~$40B+ destroyed
The UST algorithmic stablecoin depegged; the mint/burn mechanism hyperinflated LUNA from ~$80 to ~$0 in days, erasing $40B+ and triggering contagion (Celsius, 3AC, Voyager).
**Security root cause:** economic/design insecurity — a reflexive peg with no exogenous collateral. (Founder later charged/pursued across jurisdictions.)
**cryptoatlas angle:** nation-state/treasury exposure + sanctioned/charged-entity tracking; contagion mapped via exchange flows.

## 5. Ronin Bridge (2022) — validator compromise, $625M
Axie Infinity’s Ronin bridge lost ~173k ETH + 25.5M USDC. Attackers (attributed to North Korea’s **Lazarus Group**) controlled 5 of 9 validator keys via social engineering. RON/AXS fell hard.
**Security root cause:** over-centralized multisig (5/9), human-factor key compromise.
**cryptoatlas angle:** **OFAC sanctioned the attacker address (Aug 2022)** — present in cryptoatlas’s sanctioned set. The canonical bridge-risk + sanctions case.

## 6. Wormhole (2022) — signature bug, $325M
A signature-verification flaw on the Solana↔Ethereum bridge let an attacker mint 120k wETH unbacked (~$325M). Jump Crypto backfilled to prevent a SOL-ecosystem cascade.
**Security root cause:** improper validation of guardian signatures after a code change.
**cryptoatlas angle:** bridge-contract labels; bridges are the single largest loss category in crypto.

## 7. FTX (2022) — custody/fraud collapse, ~$8B hole
Not a hack: commingled customer funds, an ~$8B shortfall, and a bank-run on FTT. FTT went to near-zero, BTC dropped ~25% in a week, and trust in centralized venues cratered. (Founder convicted of fraud.)
**Security root cause:** zero segregation of customer assets, no real reserves, related-party self-dealing.
**cryptoatlas angle:** exchange wallets + seizure/forfeiture addresses (DOJ recovered assets are tracked publicly).

## 8. Poly Network (2021) — cross-chain exploit, $611M
A flaw in cross-chain contract calls let an attacker drain ~$611M across three chains — the largest DeFi exploit at the time. Most funds were returned after negotiation.
**Security root cause:** privileged cross-chain “keeper” call could be spoofed; access-control failure.
**cryptoatlas angle:** multi-chain contract labels; demonstrates correlated risk across an entity’s deployments.

## 9. Nomad Bridge (2022) — flawed upgrade, $190M
A botched contract upgrade marked a zero root as valid, turning the bridge into a “free-for-all” — hundreds of copy-paste drainers took ~$190M in hours.
**Security root cause:** an initialization/upgrade error that broke message verification.
**cryptoatlas angle:** bridge-contract category; the dozens of drainer addresses are publicly labeled.

## 10. Euler Finance (2023) — flash-loan attack, $197M
A donation/liquidation logic flaw in the lending protocol was exploited via flash loans for ~$197M. After on-chain negotiation, nearly all funds were returned.
**Security root cause:** missing health-check on a donation path; composability risk.
**cryptoatlas angle:** DeFi-protocol labels; shows how a single contract bug reprices a protocol’s token overnight.

---

## Synthesis — why most chains haven’t “popped,” and keep having big drops

1. **Bridges are the attack surface.** Cross-chain bridges account for several of the largest losses ever (Ronin, Wormhole, Poly, Nomad — billions combined). Every new L1/L2 needs a bridge to bootstrap liquidity, and that bridge is the soft underbelly. The market prices this in as a discount.
2. **Exploits permanently impair trust and liquidity.** A hack doesn’t just lose funds — it drains TVL, scares LPs, and the token rarely fully recovers its pre-hack multiple. The overhang lingers for years (Mt. Gox).
3. **Design/economic insecurity is as deadly as code bugs.** Terra showed a “secure” codebase can still implode if the economic mechanism is reflexive. Depegs cause contagion that drags the whole sector.
4. **Centralized custody keeps failing the same way.** Mt. Gox, Coincheck, FTX — different decades, same root cause: no segregation, no proof-of-reserves, single points of failure. Each event resets sector-wide trust lower.
5. **Sanctions and seizures remove supply but signal risk.** OFAC tagging (Lazarus/Ronin, Tornado Cash) and DOJ seizures pull coins out of circulation, but also mark a chain/protocol as compromised — institutions price that as headline risk.
6. **The result: an unpriced (then suddenly priced) security discount.** Most chains carry latent security risk the market ignores in bull euphoria and brutally re-prices on the next exploit. That asymmetry — slow trust-building, instant trust-destruction — is why so many chains drop hard and don’t “pop” the way their roadmaps promise.

**How to use cryptoatlas for this:** monitor the `sanctioned`, `seizure`, `bridge`, and `mixer` categories as a live risk signal — flows into tagged addresses, new sanctions entries, and bridge-contract activity are leading indicators of the events above. Security risk *is* market risk in crypto.
