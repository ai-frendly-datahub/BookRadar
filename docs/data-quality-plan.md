# Data Quality Plan

- 생성 시각: `2026-04-11T16:05:37.910248+00:00`
- 우선순위: `P2`
- 데이터 품질 점수: `77`
- 가장 약한 축: `운영 깊이`
- Governance: `low`
- Primary Motion: `intelligence`

## 현재 이슈

- 가장 약한 품질 축은 운영 깊이(55)

## 필수 신호

- 도서 판매 랭킹과 순위 변동
- 도서관 대출·예약·소장 데이터
- 작가 행사·출간 일정·수상 이력

## 품질 게이트

- ISBN을 canonical key로 우선 사용
- 리뷰/기사와 판매/대출 신호를 분리
- 판본·번역본·전자책/종이책을 별도 edition으로 유지

## 다음 구현 순서

- 판매 랭킹 추이와 도서관 대출 source를 운영 레이어로 추가
- ISBN/edition canonicalization rule을 추가
- 작가 행사와 수상 이력을 demand validation 보조 신호로 연결

## 운영 규칙

- 원문 URL, 수집일, 이벤트 발생일은 별도 필드로 유지한다.
- 공식 source와 커뮤니티/시장 source를 같은 신뢰 등급으로 병합하지 않는다.
- collector가 인증키나 네트워크 제한으로 skip되면 실패를 숨기지 말고 skip 사유를 기록한다.
- 이 문서는 `scripts/build_data_quality_review.py --write-repo-plans`로 재생성한다.
