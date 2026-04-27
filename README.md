# BookRadar - 도서 시장 레이더

**🌐 Live Report**: https://ai-frendly-datahub.github.io/BookRadar/

도서 시장 데이터를 수집하고 장르별, 출판사별 트렌드를 분석합니다.

## 프로젝트 목표

- **데이터 수집**: 알라딘, YES24, 교보문고 RSS 피드
- **엔티티 분석**: 장르, 출판사, 도서 유형, 수상 정보 키워드 매칭
- **트렌드 리포트**: DuckDB 저장 + HTML 리포트로 도서 시장 동향 시각화
- **자동화**: GitHub Actions 일일 수집 + GitHub Pages 리포트 자동 배포

## 기술적 우수성

- **안정성**: HTTP 자동 재시도(지수 백오프), 서킷 브레이커 패턴, DB 트랜잭션 에러 처리
- **관찰성**: 구조화된 JSON 로깅으로 파이프라인 상태 실시간 모니터링
- **품질 보증**: 단위 테스트로 코드 변경 시 회귀 버그 사전 차단
- **고성능**: 멀티스레드 수집, 적응형 스로틀링으로 대량 데이터 수집 시 성능 향상
- **운영 자동화**: Email/Webhook 알림으로 무인 운영 가능
- **공유 아키텍처**: radar-core 라이브러리로 코드 재사용성 극대화

## 빠른 시작

1. 가상환경을 만들고 의존성을 설치합니다.
   ```bash
   pip install -r requirements.txt
   ```

2. 실행:
   ```bash
   python main.py --category book --recent-days 7
   # 리포트: reports/book_report.html
   ```

   주요 옵션: `--per-source-limit 20`, `--recent-days 5`, `--keep-days 60`, `--timeout 20`.

## 수집 소스

BookRadar는 다음 RSS 피드에서 도서 정보를 수집합니다:

- **알라딘 신간**: 최신 출간 도서 정보
- **알라딘 베스트셀러**: 판매 순위 기반 인기 도서
- **YES24 베스트셀러**: 종합 베스트셀러 순위
- **교보문고 북뉴스**: 도서 관련 뉴스 및 신간 소식

## 엔티티 분석

수집된 기사에서 다음 엔티티를 자동으로 추출합니다:

- **장르**: 소설, 에세이, 경제경영, 자기계발, 인문학, 과학, 역사, 판타지, 추리 등
- **출판사**: 민음사, 창비, 문학동네, 김영사, 위즈덤하우스, 한빛미디어 등
- **도서 유형**: 신간, 베스트셀러, 화제의 책, 전자책, 오디오북 등
- **수상/선정**: 문학상, 올해의 책, 노벨상, 부커상, 퓰리처상 등

## GitHub Actions & GitHub Pages

- 워크플로: `.github/workflows/radar-crawler.yml`
  - 스케줄: 매일 00:00 UTC (KST 09:00), 수동 실행도 지원.
  - 환경 변수 `RADAR_CATEGORY`를 프로젝트에 맞게 수정하세요.
  - 리포트 배포 디렉터리: `reports` → `gh-pages` 브랜치로 배포.
  - DuckDB 경로: `data/radar_data.duckdb` (Pages에 올라가지 않음). 아티팩트로 7일 보관.

- 설정 방법:
  1) 저장소 Settings → Pages에서 `gh-pages` 브랜치를 선택해 활성화
  2) Actions 권한을 기본값으로 두거나 외부 PR에서도 실행되도록 설정
  3) 워크플로 파일의 `RADAR_CATEGORY`를 원하는 YAML 이름으로 변경

## 동작 방식

- **수집**: 카테고리 YAML에 정의된 소스를 수집합니다. 실행 시 DuckDB에 적재하고 보존 기간(`keep_days`)을 적용합니다.
- **분석**: 엔티티별 키워드 매칭. 매칭된 키워드를 리포트에 칩으로 표시합니다.
- **리포트**: `reports/<category>_report.html`을 생성하며, 최근 N일(기본 7일) 기사와 엔티티 히트 카운트, 수집 오류를 표시합니다.

## 기본 경로

- DB: `data/radar_data.duckdb`
- 리포트 출력: `reports/`

## 디렉터리 구성

```
BookRadar/
  main.py                 # CLI 엔트리포인트
  requirements.txt        # 의존성
  config/
    config.yaml           # DB/리포트 경로 설정
    categories/
      book.yaml           # 소스 + 엔티티 정의
  bookradar/
    collector.py          # 데이터 수집 (RSS 피드)
    analyzer.py           # 엔티티 태깅 (radar-core 위임)
    reporter.py           # HTML 렌더링 (radar-core 위임)
    storage.py            # DuckDB 저장/정리 (radar-core 위임)
    config_loader.py      # YAML 로더
    models.py             # 데이터 클래스 (radar-core 재사용)
  .github/workflows/      # GitHub Actions (crawler + Pages 배포)
```

## 개발

테스트 실행:
```bash
pytest tests/ -v
```

## 아키텍처

BookRadar는 radar-core 공유 라이브러리를 활용합니다:

- `from radar_core.models import Article, CategoryConfig`
- `from radar_core.storage import RadarStorage`
- `from radar_core.analyzer import apply_entity_rules`
- `from radar_core.report_utils import generate_report, generate_index_html`

이를 통해 코드 중복을 최소화하고 여러 Radar 프로젝트 간 일관성을 유지합니다.

<!-- DATAHUB-OPS-AUDIT:START -->
## DataHub Operations

- CI/CD workflows: `radar-crawler.yml`.
- GitHub Pages visualization: `reports/index.html` (valid HTML); https://ai-frendly-datahub.github.io/BookRadar/.
- Latest remote Pages check: HTTP 200, HTML.
- Local workspace audit: 23 Python files parsed, 0 syntax errors.
- Re-run audit from the workspace root: `python scripts/audit_ci_pages_readme.py --syntax-check --write`.
- Latest audit report: `_workspace/2026-04-14_github_ci_pages_readme_audit.md`.
- Latest Pages URL report: `_workspace/2026-04-14_github_pages_url_check.md`.
<!-- DATAHUB-OPS-AUDIT:END -->
