# Business Quality Upgrade

- Generated: `2026-04-14T04:48:11.525239+00:00`
- Portfolio verdict: `충분`
- Business value score: `76.6`
- Upgrade phase: P2 판매/대출 운영 신호 강화
- Primary motion: `intelligence`
- Weakest dimension: `operational_depth`

## Current Evidence

- Primary rows: `1601`
- Today raw rows: `19`
- Latest report items: `35`
- Match rate: `100.0%`
- Collection errors: `0`
- Freshness gap: `0`

## Upgrade Actions

- sales_ranking과 library_lending source를 운영 레이어 후보로 검증한다.
- ISBN/edition canonicalization으로 판본/번역본 중복을 분리한다.
- author_event와 award_signal은 판매/대출 신호를 설명하는 보조 이벤트로 유지한다.

## Quality Contracts

- `config/categories/book.yaml`: output `reports/book_quality.json`, tracked `sales_ranking, library_lending, author_event, award_signal`, backlog items `4`

## Contract Gaps

- None.
