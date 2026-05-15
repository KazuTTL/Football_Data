# Data Warehouse Schema (MotherDuck)

Bản mô tả chi tiết cấu trúc dữ liệu đang lưu trữ tại Data Warehouse **MotherDuck**, database `football_data`.

---

## 🥈 Tầng Silver (Hiện tại)
Bảng dữ liệu ở tầng này là bảng phẳng (Flat Table), đã được làm sạch và hợp nhất từ nhiều nguồn.

### Bảng: `silver_players`
Đây là bảng lưu trữ thông tin cầu thủ đã qua xử lý, bao gồm cả lịch sử thay đổi (SCD Type 2).

| Nhóm cột | Tên cột | Kiểu dữ liệu | Mô tả |
|:--- |:--- |:--- |:--- |
| **Định danh** | `internal_player_id` | VARCHAR | ID duy nhất toàn hệ thống (PLR_xxxxx) |
| | `id_sfs` | VARCHAR | ID gốc từ Sofascore |
| | `id_tm` | VARCHAR | ID gốc từ Transfermarkt |
| **Thông tin cá nhân** | `name_sfs_raw` | VARCHAR | Tên cầu thủ (gốc) |
| | `name_sfs_norm` | VARCHAR | Tên cầu thủ (đã chuẩn hóa, không dấu) |
| | `dob_tm` | DATE | Ngày sinh (YYYY-MM-DD) |
| | `team_tm` | VARCHAR | Câu lạc bộ hiện tại |
| **Vị trí thi đấu** | `position_tm` | VARCHAR | Nhóm vị trí (Attack, Defender, Midfielder, Goalkeeper) |
| | `sub_position_tm` | VARCHAR | Vị trí cụ thể (Centre-Forward, Centre-Back...) |
| **Chỉ số hiệu suất** | `goals_sfs` | FLOAT | Tổng số bàn thắng trong mùa giải |
| | `assists_sfs` | FLOAT | Tổng số kiến tạo trong mùa giải |
| | `market_value_tm` | FLOAT | Giá trị thị trường (Đơn vị: EUR) |
| **Quản lý lịch sử (SCD2)**| `is_current` | BOOLEAN | `True` nếu là dữ liệu mới nhất của cầu thủ |
| | `valid_from` | DATE | Ngày bắt đầu có hiệu lực của bản ghi |
| | `valid_to` | DATE | Ngày hết hiệu lực (NULL nếu đang là bản ghi mới nhất) |
| **Dòng chảy dữ liệu** | `updated_at_sfs` | DATE | Ngày cập nhật từ API |
| | `updated_at_tm` | DATE | Ngày cập nhật từ file CSV |
| | `synced_at` | TIMESTAMP | Thời điểm đồng bộ lên Cloud MotherDuck |

---

## 🥇 Tầng Gold (Star Schema)

Dữ liệu được tách thành 5 bảng Dimension + 1 bảng Fact theo mô hình Star Schema.

### 1. Các bảng Dimension

#### `dim_player` — Thông tin cầu thủ (SCD Type 2)

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| `player_key` | INTEGER | Surrogate Key (khóa thay thế) |
| `internal_player_id` | VARCHAR | ID gốc từ Silver Zone (PLR_xxxxx) |
| `name` | VARCHAR | Tên cầu thủ |
| `dob` | DATE | Ngày sinh |
| `nationality` | VARCHAR | Quốc tịch |
| `sub_position` | VARCHAR | Vị trí chi tiết (Centre-Forward, Defensive Midfield...) |
| `current_market_value` | FLOAT | Giá trị thị trường hiện tại (EUR) |
| `is_current` | BOOLEAN | True nếu là bản ghi mới nhất |
| `valid_from` | DATE | Ngày bắt đầu hiệu lực |
| `valid_to` | DATE | Ngày hết hiệu lực (NULL nếu đang active) |

#### `dim_team` — Thông tin câu lạc bộ

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| `team_key` | INTEGER | Surrogate Key |
| `name` | VARCHAR | Tên đội bóng |
| `short_name` | VARCHAR | Tên viết tắt |
| `primary_color` | VARCHAR | Màu chủ đạo (hex) |
| `secondary_color` | VARCHAR | Màu phụ (hex) |

#### `dim_tournament` — Giải đấu

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| `tournament_key` | INTEGER | Surrogate Key |
| `name` | VARCHAR | Tên giải đấu (Premier League, La Liga...) |
| `country` | VARCHAR | Quốc gia |

#### `dim_season` — Mùa giải

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| `season_key` | INTEGER | Surrogate Key |
| `name` | VARCHAR | Tên mùa (2024-25, 2025-26...) |
| `start_date` | DATE | Ngày bắt đầu |
| `end_date` | DATE | Ngày kết thúc |

#### `dim_position` — Vị trí & Trọng số Rating

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| `position_key` | INTEGER | Surrogate Key |
| `name` | VARCHAR | Tên vị trí (Centre-Forward, Left-Winger...) |
| `general_position` | VARCHAR | Nhóm chính (Attack, Midfield, Defender, Goalkeeper) |
| `role_weights` | JSON | Trọng số chấm điểm (tham khảo `ratingEngine.md`) |

> `role_weights` là JSON chứa toàn bộ cấu hình weights + penalty cho từng sub-position:
> ```json
> {
>   "metrics": {"goals_p90": 0.40, "xg_p90": 0.15, "sot_p90": 0.10},
>   "penalties": {"possession_lost": -5, "big_chances_missed": -5},
>   "team_bonus_alpha": 0.015,
>   "threshold_minutes": 900
> }
> ```

---

### 2. Bảng Fact

#### `fact_player_season_stats` — Hiệu suất cầu thủ theo mùa

**Grain**: Mỗi dòng = 1 cầu thủ + 1 mùa giải + 1 đội bóng

| Nhóm cột | Cột | Kiểu | Mô tả |
|:--- |:--- |:--- |:--- |
| **Khóa ngoại (FK)** | `player_key` | INTEGER | → dim_player.player_key |
| | `team_key` | INTEGER | → dim_team.team_key |
| | `tournament_key` | INTEGER | → dim_tournament.tournament_key |
| | `season_key` | INTEGER | → dim_season.season_key |
| | `position_key` | INTEGER | → dim_position.position_key |
| **Thông số lõi** | `minutes_played` | INTEGER | Tổng số phút thi đấu (dùng cho threshold & P90) |
| | `team_rank` | INTEGER | Thứ hạng đội cuối mùa (dùng cho Underdog Bonus) |
| **Chỉ số thô (Raw Metrics)** | `goals` | FLOAT | Bàn thắng |
| | `assists` | FLOAT | Kiến tạo |
| | `xg` | FLOAT | Expected Goals |
| | `xa` | FLOAT | Expected Assists |
| | `shots_on_target` | FLOAT | Sút trúng đích |
| | `big_chances_created` | FLOAT | Cơ hội lớn tạo ra |
| | `big_chances_missed` | FLOAT | Cơ hội lớn bỏ lỡ |
| | `key_passes` | FLOAT | Đường chuyền then chốt |
| | `successful_dribbles` | FLOAT | Qua người thành công |
| | `tackles` | FLOAT | Tắc bóng |
| | `interceptions` | FLOAT | Đánh chặn |
| | `clearances` | FLOAT | Phá bóng |
| | `possession_lost` | FLOAT | Mất bóng |
| | `ground_duels_won` | FLOAT | Thắng tranh chấp mặt đất |
| | `aerial_duels_won` | FLOAT | Thắng tranh chấp trên không |
| | `accurate_passes` | FLOAT | Chuyền chính xác |
| | `total_passes` | FLOAT | Tổng chuyền |
| | `goal_conversion` | FLOAT | Tỷ lệ chuyển hóa bàn thắng |
| | `errors_lead_to_goal` | FLOAT | Lỗi dẫn đến bàn thua |
| | `dribbled_past` | FLOAT | Bị qua người |
| | `saves` | FLOAT | Cứu thua |
| | `clean_sheets` | FLOAT | Giữ sạch lưới |
| | `goals_conceded` | FLOAT | Bàn thua |
| **Mảng (Array)** | `recent_5_ratings_array` | FLOAT[] | Mảng 5 điểm số trận gần nhất (dùng vẽ sparkline) |
| **Đầu ra** | `final_scout_score` | FLOAT | Điểm rating cuối cùng (0-100) từ Rating Engine |

---

### 3. Mối quan hệ (Star Schema)

```
                    ┌──────────────┐
                    │  dim_player  │
                    └──────┬───────┘
                           │
          ┌────────────┐   │   ┌──────────────┐
          │  dim_team  │───┼───│  dim_season  │
          └────────────┘   │   └──────────────┘
                           │
                    ┌──────▼───────┐
                    │ fact_player_ │
                    │ season_stats │
                    └──────┬───────┘
                           │
          ┌────────────┐   │   ┌──────────────┐
          │dim_tourna- │───┼───│ dim_position │
          │   ment     │   │   └──────────────┘
          └────────────┘   │
                           │
                    ┌──────▼───────┐
                    │gold_player_  │
                    │   rating     │
                    └──────────────┘
```

---

## 🛡️ Ràng buộc dữ liệu (Data Constraints)

| Loại | Mô tả |
|------|-------|
| **Primary Key (Fact)** | `player_key` + `season_key` + `team_key` |
| **Primary Key (Dim)** | `{dim}_key` (Surrogate Key tự tăng) |
| **Foreign Key** | Các `*_key` trong Fact phải tồn tại trong Dim tương ứng |
| **Data Quality** | `minutes_played` ≥ 0; `team_rank` 1-20; `final_scout_score` 0-100 |
| **Unique (Silver)** | `internal_player_id` + `valid_from` (đảm bảo SCD2) |

---

## 🔄 Flow dữ liệu (Bronze → Silver → Gold)

```
Phase 1 (Bronze)         Phase 2 (Silver)              Phase 3 (Gold)
─────────────────      ──────────────────           ──────────────────
Sofascore JSON   ──→  silver_players      ──→  dim_player + dim_team
Transfermarkt CSV──→  (flat table, SCD2)        dim_tournament + dim_season
                                                 dim_position
                                                    │
                                           ┌───────▼────────┐
                                           │rating_engine.py│
                                           └───────┬────────┘
                                                    │
                                        fact_player_season_stats
                                        (final_scout_score computed)
```
