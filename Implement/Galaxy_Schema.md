# Galaxy Schema - Mở rộng sau (Phase 4+)

Tài liệu này mô tả hướng mở rộng từ Star Schema hiện tại lên Galaxy Schema (Fact Constellation) khi dự án phát triển và có thêm dữ liệu.

---

## 🎯 Khi nào mở rộng?

| Điều kiện | Mô tả |
|-----------|-------|
| Dataset > 50 cầu thủ/vị trí | Có thể dùng Percentile thay Absolute Scoring |
| Có dữ liệu match-level (từng trận) | Cần thêm `fact_player_match_stats` |
| Cần theo dõi biến động giá trị | Cần thêm `fact_market_value_history` |
| Dashboard yêu cầu phân tích match-level | Biểu đồ phong độ theo trận, heatmap |

---

## 📦 Kiến trúc Galaxy Schema (3 Fact + 7 Dim)

```
┌────────────┐   ┌────────────┐   ┌────────────┐   ┌────────────┐
│ dim_player │   │  dim_team  │   │dim_tourna- │   │ dim_season │
│            │   │            │   │   ment     │   │            │
└─────┬──────┘   └─────┬──────┘   └─────┬──────┘   └─────┬──────┘
      │                │                │                │
      └────────┬───────┴────────────────┴────────────────┘
               │
      ┌────────▼──────────────────────────────────────────┐
      │           fact_player_season_stats (Grain: Season)│
      │  (đã có trong Star Schema - giữ nguyên)           │
      └───────────────────────────────────────────────────┘

      ┌───────────────────────────────────────────────────┐
      │           fact_player_match_stats (Grain: Match)  │
      │  - player_key, match_key, team_key               │
      │  - minutes_played, goals, assists, tackles,...   │
      │  - match_rating (rating từng trận)               │
      └───────────────────────────────────────────────────┘

      ┌───────────────────────────────────────────────────┐
      │         fact_market_value_history (Grain: Date)   │
      │  - player_key, date_key                          │
      │  - market_value_eur, delta_from_previous         │
      └───────────────────────────────────────────────────┘

┌────────────┐   ┌────────────┐
│ dim_match  │   │  dim_date  │
│ (thêm mới) │   │ (thêm mới) │
└────────────┘   └────────────┘
```

---

## 1. Bảng Dim bổ sung

### `dim_match` — Metadata trận đấu

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| `match_key` | INTEGER | Surrogate Key |
| `tournament_key` | INTEGER | FK → dim_tournament |
| `season_key` | INTEGER | FK → dim_season |
| `home_team_key` | INTEGER | FK → dim_team (đội nhà) |
| `away_team_key` | INTEGER | FK → dim_team (đội khách) |
| `match_date` | DATE | Ngày diễn ra |
| `home_score` | INTEGER | Tỷ số đội nhà |
| `away_score` | INTEGER | Tỷ số đội khách |
| `status` | VARCHAR | Finished / Postponed / Cancelled |

### `dim_date` — Trục thời gian

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| `date_key` | INTEGER | YYYYMMDD |
| `full_date` | DATE | Ngày đầy đủ |
| `day` | INTEGER | Ngày trong tháng |
| `month` | INTEGER | Tháng |
| `quarter` | INTEGER | Quý |
| `year` | INTEGER | Năm |
| `is_weekend` | BOOLEAN | True nếu là Thứ 7 / Chủ nhật |

---

## 2. Bảng Fact bổ sung

### `fact_player_match_stats` — Chỉ số từng trận

**Grain**: Mỗi dòng = 1 cầu thủ + 1 trận đấu

| Nhóm cột | Cột | Kiểu | Mô tả |
|:--- |:--- |:--- |:--- |
| **FK** | `player_key` | INTEGER | → dim_player |
| | `match_key` | INTEGER | → dim_match |
| | `team_key` | INTEGER | → dim_team |
| | `position_key` | INTEGER | → dim_position |
| **Thông số** | `minutes_played` | INTEGER | Số phút thi đấu trong trận |
| | `goals` | FLOAT | Bàn thắng |
| | `assists` | FLOAT | Kiến tạo |
| | `xg` | FLOAT | Expected Goals |
| | `xa` | FLOAT | Expected Assists |
| | `tackles` | FLOAT | Tắc bóng |
| | `interceptions` | FLOAT | Đánh chặn |
| | `successful_dribbles` | FLOAT | Qua người |
| | `key_passes` | FLOAT | Đường chuyền then chốt |
| | `saves` | FLOAT | Cứu thua (GK) |
| | `clean_sheet` | BOOLEAN | Giữ sạch lưới |
| | `match_rating` | FLOAT | Điểm Sofascore trận đó |
| | `match_scout_score` | FLOAT | Điểm Rating Engine cho trận đó |

### `fact_market_value_history` — Lịch sử giá trị

**Grain**: Mỗi dòng = 1 cầu thủ + 1 ngày cập nhật

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| `player_key` | INTEGER | FK → dim_player |
| `date_key` | INTEGER | FK → dim_date |
| `market_value_eur` | FLOAT | Giá trị tại thời điểm đó |
| `delta_from_previous` | FLOAT | Chênh lệch so với lần trước |
| `source` | VARCHAR | Transfermarkt / Internal Estimate |

---

## 3. Lợi ích khi mở rộng

### Với `fact_player_match_stats`
- Vẽ biểu đồ **phong độ theo trận** (line chart)
- Phát hiện **chuỗi trận tốt/xấu**
- Tính **consistency score** (độ ổn định)

### Với `fact_market_value_history`
- Vẽ biểu đồ **biến động giá trị** theo thời gian
- So sánh: **giá trị tăng** khi rating tăng? (validation)
- Phát hiện: **cầu thủ đang lên / đang xuống**

### Với `dim_date`
- Time intelligence: So sánh **cùng kỳ năm trước**
- Tính **moving average**, **YoY growth**

---

## 4. Cập nhật Rating Engine (khi có match-level data)

Khi có `fact_player_match_stats`, Rating Engine có thể nâng cấp:

| Hiện tại (Season-level) | Nâng cao (Match-level) |
|-------------------------|----------------------|
| Rating cố định 1 mùa | Rating thay đổi theo từng trận |
| Không biết phong độ gần | `recent_5_matches_avg` luôn cập nhật |
| Underdog bonus theo team rank cố định | Underdog bonus theo sức mạnh đối thủ từng trận |

---

## 5. Kế hoạch nâng cấp

| Giai đoạn | Việc cần làm |
|-----------|-------------|
| Phase 3 (hiện tại) | Star Schema: 5 Dim + 1 Fact + Rating Engine |
| Phase 4 | Thu thập dữ liệu match-level từ Sofascore API |
| Phase 4 | Tạo `dim_match` + `dim_date` |
| Phase 4 | Tạo `fact_player_match_stats` + `fact_market_value_history` |
| Phase 5 | Nâng cấp Rating Engine: Match-level + Time intelligence |
| Phase 5 | Dashboard: Sparkline, Trend, Consistency Score |
