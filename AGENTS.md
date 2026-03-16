# BOOKRADAR

도서 시장 데이터를 수집하고 장르별, 출판사별 트렌드를 분석합니다.

## STRUCTURE

```
BookRadar/
├── bookradar/
│   ├── collector.py              # collect_sources() — 알라딘, YES24, 교보문고 RSS 수집
│   ├── analyzer.py               # apply_entity_rules() — 장르, 출판사, 도서 유형, 수상 정보 키워드 매칭
│   ├── reporter.py               # generate_report() — Jinja2 HTML (radar-core 위임)
│   ├── storage.py                # RadarStorage — DuckDB upsert/query/retention (radar-core 위임)
│   ├── models.py                 # Source, Article, EntityDefinition, CategoryConfig (radar-core 재사용)
│   ├── config_loader.py          # YAML 로딩
│   ├── logger.py                 # structlog 구조화 로깅
│   ├── resilience.py             # 서킷 브레이커 패턴
│   └── exceptions.py             # 커스텀 예외 클래스
├── config/
│   ├── config.yaml               # database_path, report_dir
│   └── categories/book.yaml      # 소스 + 엔티티 정의
├── data/                         # DuckDB, crawl health 데이터
├── reports/                      # 생성된 HTML 리포트
├── tests/unit/                   # pytest 단위 테스트
├── main.py                       # CLI 엔트리포인트
└── .github/workflows/radar-crawler.yml
```

## ENTITIES

| Entity | Examples |
|--------|----------|
| 장르 | 소설, 에세이, 경제경영, 자기계발, 인문학, 과학, 역사, 판타지, 추리, SF |
| 출판사 | 민음사, 창비, 문학동네, 김영사, 위즈덤하우스, 한빛미디어 |
| 도서 유형 | 신간, 베스트셀러, 화제의 책, 전자책, 오디오북 |
| 수상/선정 | 문학상, 올해의 책, 노벨상, 부커상, 퓰리처상 |

## DEVIATIONS FROM TEMPLATE

- **radar-core 의존성**: 모델, 스토리지, 분석, 리포트 생성 로직을 radar-core 공유 라이브러리에서 가져옴
- **적응형 스로틀링**: AdaptiveThrottler로 소스별 요청 간격을 동적 조정
- **서킷 브레이커**: 장애 소스를 자동으로 격리하여 전체 파이프라인 안정성 확보
- **크롤 헬스 추적**: CrawlHealthStore로 소스별 성공/실패율 모니터링 및 자동 비활성화
- **EUC-KR 인코딩 지원**: 한국 .kr 도메인의 레거시 인코딩 자동 감지 및 변환

## COMMANDS

```bash
python main.py --category book --recent-days 7
python main.py --category book --per-source-limit 50 --keep-days 90
```

## 주의사항

- **DuckDB 스키마 변경 금지**: radar-core와 호환성 유지 필요
- **`generate_report()` 함수 시그니처 변경 금지**: 다른 Radar 프로젝트와 공유
- **config/categories/book.yaml 수정 시**: 엔티티 키워드 추가는 가능하나 구조 변경은 신중히
- **collector.py 수집 로직**: RSS 피드 파싱 로직은 feedparser 라이브러리에 의존
